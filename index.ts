import { randomUUID } from 'node:crypto';

type EscapeFn = (value: any) => string;

let escape: EscapeFn = (value: any) => {
  throw new Error('Must register an escape function');
}

export function registerEscaper(fn: EscapeFn) {
  escape = fn;
}

export interface IHasUUID {
  id: string;
}

export interface IHasAlias extends IHasUUID {
  alias: string;
}

type ExpressionArg<T> = QCol<T> | T;

export class Table<T> {
  public model: T;
  constructor(
    public name: string,
    public ctor: new () => T
  ) {
    this.model = new this.ctor();
  }
}

class QCol<T>{
  public useAlias = false

  public id = randomUUID();
  public value?: QCol<T>;
  public alias = '';

  constructor(
    public path: string,
    public parent?: QTable<unknown>,
    public defer?: (q: Query) => string
  ) { }

  get ref() {
    return this.useAlias ? this.alias : this.path;
  }

  as(alias = '') {
    this.useAlias = true;

    if (alias)
      this.alias = alias;

    return this;
  }

}

function operation<T>(fn: (q: Query) => string) {
  return new QCol<T>('', undefined, fn);
}

function subquery<T>(value: IQueryable) {

  return new QCol<T>('', undefined, (q) => {

    const output = value.toSql({
      ptr_var: q.ptr_var,
      ptr_table: q.ptr_table
    });

    q.ptr_var = value.q.ptr_var;
    q.ptr_table = value.q.ptr_table;

    return output;

  })
}

function toCol<T>(q: Query, obj: ExpressionArg<T>) {
  if (obj instanceof QCol)
    return obj;
  else if (typeof obj === 'string' || typeof obj === 'number' || typeof obj === 'boolean')
    return new QCol<T>(q.paramaterize(obj))
  else
    throw new Error(`Cannot convert ${JSON.stringify(obj)} to QCol`);
}

type QColMap<T> = { [Index in keyof T]: QCol<T[Index]> }
type QExpressionMap<T> = { [Index in keyof T]: QCol<T[Index]> | T[Index] };

class QTable<T> {

  public readonly id = randomUUID();
  public readonly cols: QColMap<T>;

  constructor(
    public base: T,
    public path: string,
    public alias: string = '',
    cols?: QColMap<T>
  ) {

    if (cols)
      this.cols = cols;
    else
      this.cols = Object.fromEntries(Object.entries(base).map(pair => [pair[0], new QCol<unknown>(pair[0], this)])) as QColMap<T>;

  }
}

type QType = 'SELECT' | 'INSERT' | 'UPDATE' | 'DELETE';

export class Query {

  ptr_var = 1;
  ptr_table = 1;

  aliases = new Map<string, string>();
  col_store = new Map<string, { path: string; alias: string; }>();

  scope: QTable<unknown>[] = [];
  variables: string[] = [];
  selected: Record<string, QCol<unknown>> = {};
  wheres: QCol<unknown>[] = [];
  joins: QCol<void>[] = [];

  insert = {
    cols: {} as QColMap<unknown>,
    values: [] as QCol<unknown>[]
  }

  updates: [QCol<unknown>, QCol<unknown>][] = [];

  deletes: QTable<unknown>[] = [];

  constructor(
    private from: QTable<unknown>,
    private type: QType
  ) {
    this.scope = [from];
  }

  public tableAlias(table: QTable<unknown>) {
    let alias = this.aliases.get(table.id);

    if (!alias) {
      alias = table.alias || `T${this.ptr_table++}`;
      table.alias = alias;
      this.aliases.set(table.id, table.alias);
    }

    return table.alias;
  }

  public colRef(col: QCol<unknown>) {

    let stored = this.col_store.get(col.id);
    if (!stored) {
      let path = col.path;

      if (col.defer)
        path = col.defer(this);
      else if (col.parent)
        path = `${this.tableAlias(col.parent)}.${col.path}`;

      stored = { path, alias: col.useAlias ? col.alias : '' };

      this.col_store.set(col.id, stored);

      return stored.path;

    } else {
      return stored.alias || stored.path;
    }


    // if (col.useAlias)
    //   return col.alias;
    // else if (col.parent)
    //   return `${this.tableAlias(col.parent)}.${col.path}`;
    // else
    //   return col.path;
  }

  public paramaterize(value: string | number | boolean) {
    const escaped = escape(value);
    const binding = `@value_${this.ptr_var++}`;
    const str = `${binding} = ${escaped};`;
    this.variables.push(str);
    return binding;
  }

  private selectStr({ from = this.from } = {}) {
    let q = `SELECT ${(Object.values(this.selected) as QCol<unknown>[]).map((col) => {
      let stored = this.col_store.get(col.id);

      if (stored) {
        return stored.alias || stored.path;
      }
      else {
        let str = col.path;

        if (col.defer) {
          str = col.defer(this);
        }
        else {
          if (col.parent)
            str = `${this.tableAlias(col.parent)}.${str}`;
        }

        this.col_store.set(col.id, { path: str, alias: col.useAlias ? col.alias : '' });

        if (col.useAlias)
          str += ` AS ${col.alias}`;

        return str;
      }

    }).join(', ')} FROM ${from.path} ${this.tableAlias(from)}`;
    return q;
  }

  private insertStr({ from = this.from } = {}) {
    let q = `INSERT INTO ${from.path} ${this.tableAlias(from)}`;
    q += `\r\n(${Object.values(this.insert.cols).map(col => this.colRef(col as QCol<unknown>)).join(', ')})`;
    q += `\r\nVALUES`;
    q += `\r\n${this.insert.values.map(col => `(${col.defer?.call(col, this) || ''})`).join('\r\n')}`;
    return q;
  }

  private updateStr({ from = this.from } = {}) {
    let q = `UPDATE ${from.path} ${this.tableAlias(from)}`;
    q += `\r\nSET ${this.updates.map(pair => `${this.colRef(pair[0])}=${this.colRef(pair[1])}`).join(', ')}`;
    return q;
  }

  private deleteStr({ from = this.from } = {}) {
    let q = `DELETE ${this.deletes.map(table => this.tableAlias(table)).join(', ')} FROM ${this.from.path} ${this.tableAlias(this.from)}`;
    return q;
  }

  toSql(opts?: { ptr_var: number; ptr_table: number }) {

    if (opts) {
      this.ptr_var = opts.ptr_var;
      this.ptr_table = opts.ptr_table
    }

    let q = '';

    switch (this.type) {
      case 'SELECT': {
        q = this.selectStr();
        break;
      }
      case 'INSERT': {
        q = this.insertStr();
        break;
      }
      case 'UPDATE': {
        q = this.updateStr();
        break;
      }
      case 'DELETE': {
        q = this.deleteStr();
        break;
      }
      default:
        throw new Error('Not implemented');
    }

    if (this.joins?.length)
      q += `\r\n${this.joins.map(join => `${join.defer!.call(join, this)}`)
        .join('\r\n')}`;

    if (this.wheres?.length)
      q += `\r\nWHERE ${this.wheres.map(clause => `(${clause.defer!.call(clause, this)})`).join(' AND ')}`;

    if (this.variables?.length)
      q = this.variables.join('\r\n') + '\r\n' + q;

    q += ';';
    return q;
  }
}

type QBaseAny<T, U, V extends QType> = Omit<QBase<T, U, V>, 'q'>

type QSelectable<T, U> = Pick<QBaseAny<T, U, 'SELECT'>, 'select' | 'innerJoin' | 'where' | 'toSql'>
type QSelected<T, U> = Pick<QSelectable<T, U>, 'where' | 'toSql'>

type QFiltered<T, U> = QSelected<T, U>

type QUpdateable<T, U> = Pick<QBaseAny<T, U, 'UPDATE'>, 'innerJoin' | 'set'>
type QUpdated<T, U> = Pick<QBaseAny<T, U, 'UPDATE'>, 'where' | 'toSql'>

type QDeletable<T, U> = Pick<QBaseAny<T, U, 'DELETE'>, 'innerJoin' | 'tables' | 'where' | 'toSql'>

type RetValJoin<T, U, V extends QType> = V extends 'SELECT' ? QSelectable<T, U> : V extends 'UPDATE' ? QUpdateable<T, U> : V extends 'DELETE' ? QDeletable<T, U> : any;

type QColTuple<T> = [QCol<T>, T];
type ValidTuples = QColTuple<string> | QColTuple<number> | QCol<boolean> | [QCol<number>, QCol<number>] | [QCol<string>, QCol<string>] | [QCol<boolean>, QCol<boolean>];

interface IQueryable {
  q: Query;
  toSql(opts?: { ptr_var: number; ptr_table: number }): string;
}

class QBase<T, TSelected, BaseType extends QType> implements IQueryable {

  constructor(
    private model: T,
    public q: Query,
    private selected: TSelected,
    private q_base: BaseType
  ) { }

  innerJoin<J, TKey, U extends Record<string, QColMap<unknown>>>(toJoin: Table<J>, t1On: (model: T) => QCol<TKey>, t2On: (model: QColMap<J>) => QCol<TKey>, join: (tModel: T, joined: QColMap<J>) => U) {

    const qTable = new QTable(toJoin.model, toJoin.name);
    this.q.scope.push(qTable);

    const model = join(this.model, qTable.cols);

    this.q.joins.push(operation((q) => `INNER JOIN ${qTable.path} ${q.tableAlias(qTable)} ON ${q.colRef(t1On(this.model))} = ${q.colRef(t2On(qTable.cols))}`));

    return new QBase(model, this.q, this.selected, this.q_base) as RetValJoin<U, TSelected, BaseType>;
  }

  select<U extends Record<string, QCol<unknown>>>(fn: (model: T) => U): QSelected<T, U> {
    const selected = fn(this.model);
    const values: [string, QCol<unknown>][] = [];

    for (const key in selected) {

      if (selected[key].useAlias && !selected[key].alias)
        selected[key].alias = key;

      values.push([key, selected[key]]);

    }

    this.q.selected = Object.fromEntries(values);
    return new QBase(this.model, this.q, selected, this.q_base);
  }

  set(fn: (model: T) => ValidTuples[]): QUpdated<T, T> {

    const tuples = fn(this.model) as [QCol<unknown>, ExpressionArg<unknown>][];

    for (const pair of tuples)
      this.q.updates.push([pair[0], toCol(this.q, pair[1])]);

    return new QBase(this.model, this.q, this.model, this.q_base);
  }

  where(fn: (model: TSelected, data: T) => QCol<unknown>): QFiltered<T, TSelected> {
    this.q.wheres.push(fn(this.selected!, this.model));
    return this;
  }

  tables(...tables: Table<unknown>[]): QDeletable<T, T> {

    const deletes: QTable<unknown>[] = [];
    for (const table of tables) {
      const toDelete = this.q.scope.find(t => t.base instanceof table.ctor);
      if (!toDelete)
        throw new Error(`Table ${table.name} not registered in query scope`);
      deletes.push(toDelete);
    }

    this.q.deletes = deletes;

    return new QBase(this.model, this.q, this.model, this.q_base);
  }

  toSql(opts?: { ptr_var: number; ptr_table: number }) {
    return this.q.toSql(opts);
  }
}


class QInsertable<T>{
  constructor(
    private model: QColMap<T>,
    private q: Query
  ) { }

  values(...values: (QExpressionMap<T> | QSelected<unknown, QColMap<T>>)[]) {

    if (!Object.keys(this.q.insert.cols).length)
      this.q.insert.cols = this.model;

    for (const value of values) {

      let col: QCol<unknown>;

      if (value instanceof QBase) {

        const orderedCols = Object.fromEntries(Object.keys(this.q.insert.cols).map(key => [key, value.q.selected[key]]));
        value.q.selected = orderedCols;

        col = subquery(value);

      } else {

        const orderedCols = Object.fromEntries(Object.keys(this.q.insert.cols).map(key => [key, (value as Record<string, any>)[key]])) as QExpressionMap<T>;
        col = new QCol<unknown>('', undefined, (q) => Object.keys(this.q.insert.cols).map((key) => {

          const val = (value as Record<string, any>)[key] as ExpressionArg<unknown>;

          return q.colRef(toCol(q, val));

        }).join(', '))

      }

      this.q.insert.values.push(col);

    }

    return this;
  }

  toSql(opts?: { ptr_var: number; ptr_table: number }) {
    return this.q.toSql(opts);
  }
}

export function from<T>(table: Table<T>): QSelectable<QColMap<T>, QColMap<T>> {
  const qTable = new QTable(table.model, table.name);
  const q = new Query(qTable, 'SELECT');
  q.selected = qTable.cols;
  return new QBase(qTable.cols, q, qTable.cols, 'SELECT');
}

export function insertInto<T>(table: Table<T>) {
  const qTable = new QTable(table.model, table.name);
  const q = new Query(qTable, 'INSERT');
  return new QInsertable(qTable.cols, q);
}

export function update<T>(table: Table<T>): QUpdateable<QColMap<T>, QColMap<T>> {
  const qTable = new QTable(table.model, table.name);
  const q = new Query(qTable, 'UPDATE');
  return new QBase(qTable.cols, q, qTable.cols, 'UPDATE');
}

export function deleteFrom<T>(table: Table<T>): QDeletable<QColMap<T>, QColMap<T>> {
  const qTable = new QTable(table.model, table.name);
  const q = new Query(qTable, 'DELETE');
  return new QBase(qTable.cols, q, qTable.cols, 'DELETE');
}

export function abs(value: ExpressionArg<number>) {
  return operation((q) => `ABS(${q.colRef(toCol(q, value))})`);
}


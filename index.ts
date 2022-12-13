import { randomUUID } from 'node:crypto';
import { RowDataPacket } from 'mysql2/promise';

type EscapeFn = (value: any) => string;

let escape: EscapeFn = () => {
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

type GetInnerType<T> = T extends QSelected<any, infer U> ? U :
  T extends QOrdered<any, infer U, any> ? U :
  never;

type QColMapToNative<T> = { [Key in keyof T]: T[Key] extends QCol<infer U> ? U : never };

export type SelectResult<T> = (QColMapToNative<GetInnerType<T>> & Omit<RowDataPacket, 'constructor'>)[];

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

export class QCol<T>{

  constructor(
    public path: string,
    public parent?: QTable<unknown>,
    public defer?: (q: Query) => string
  ) { }

  id = randomUUID();

  default?: T;

}

export enum AccessContext {
  Default,
  Where,
  Having
}

function operation<T>(fn: (q: Query) => string) {
  return new QCol<T>('', undefined, fn);
}

function operation2<T>(fn: (q: Query) => string) {
  return new QCol<T>('', undefined, fn);
}

export function subquery<T>(value: QSubquery<T>) {

  return new QCol<T>('', undefined, (q) => {

    const output = value.toSql({
      ptr_var: q.ptr_var,
      ptr_table: q.ptr_table
    });

    q.ptr_var = (value as IQueryable).q.ptr_var;
    q.ptr_table = (value as IQueryable).q.ptr_table;

    return `(${output.replace(/;/, '')})`;

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

export type QColMap<T> = { [Index in keyof T]: QCol<T[Index]> }
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
      this.cols = Object.fromEntries(Object.entries(base as {}).map(pair => [pair[0], new QCol<unknown>(pair[0], this)])) as QColMap<T>;

  }
}

type QType = 'SELECT' | 'INSERT' | 'UPDATE' | 'DELETE';

export class Query {

  ptr_var = 1;
  ptr_table = 1;

  aliases = new Map<string, string>();
  col_store = new Map<string, { path: string; alias: string; }>();

  scope: QTable<unknown>[] = [];
  selected: Record<string, QCol<unknown>> = {};
  wheres: QCol<unknown>[] = [];
  havings: QCol<unknown>[] = [];
  joins: QCol<void>[] = [];
  unions: { type?: 'ALL', op: QCol<void> }[] = [];
  groupBys: QCol<unknown>[] = [];
  orderBys: OrderByArgDirectional[] = [];

  limit?: number;
  offset?: number;

  insert = {
    cols: [] as string[],
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

  public toCol<T extends (string | number | boolean | unknown)>(arg: T) {

    if (!(typeof arg === 'string' || typeof arg === 'number' || typeof arg === 'boolean'))
      throw new Error(`Cannot convert ${JSON.stringify(arg)} to QCol`);

    return new QCol<T>(this.paramaterize(arg));

  }

  public colRef<T = unknown>(arg: QCol<T> | ExpressionArg<T>, { direct = false } = {}) {

    const col = arg instanceof QCol ? arg : this.toCol(arg);

    let stored = this.col_store.get(col.id);

    if (!stored) {
      let { path } = col;

      if (col.defer)
        path = col.defer(this);
      else if (col.parent)
        path = `${this.tableAlias(col.parent)}.${col.path}`;

      stored = { path, alias: '' };

      this.col_store.set(col.id, stored);

    }

    if (direct)
      return stored.path;

    return stored.alias || stored.path;
  }

  public paramaterize(value: string | number | boolean) {
    return escape(value);
  }

  // private selectStr({ from = this.from } = {}) {
  //   let q = `SELECT ${(Object.entries(this.selected)).map(([alias, col]) => {
  //     let stored = this.col_store.get(col.id);

  //     if (stored)
  //       return stored.alias || stored.path;

  //     else {
  //       let { path } = col;

  //       if (col.defer) {
  //         path = col.defer(this);
  //       }
  //       else {
  //         if (col.parent)
  //           path = `${this.tableAlias(col.parent)}.${path}`;
  //       }

  //       this.col_store.set(col.id, { path, alias });


  //       return `${path} AS ${alias}`;
  //     }

  //   }).join(', ')} FROM ${from.path} ${this.tableAlias(from)}`;

  //   return q;
  // }

  private selectStr({ from = this.from } = {}) {

    const colsStr = Object.entries(this.selected).reduce((str, [alias, col], i) => {

      if (i > 0)
        str += ', ';

      let stored = this.col_store.get(col.id);

      if (stored)
        return stored.alias || stored.path;

      else {
        let { path } = col;

        if (col.defer) {
          path = col.defer(this);
        }
        else {
          if (col.parent)
            path = `${this.tableAlias(col.parent)}.${path}`;
        }

        this.col_store.set(col.id, { path, alias });


        return str + `${path} AS ${alias}`;
      }

    }, '');

    return `SELECT ${colsStr} FROM ${from.path} ${this.tableAlias(from)}`;
  }

  private insertStr({ from = this.from } = {}) {
    let q = `INSERT INTO ${from.path}`;
    q += `\r\n(${this.insert.cols.join(', ')})`;
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

    if (this.joins.length)
      q += `\r\n${this.joins.map(join => `${join.defer!(this)}`)
        .join('\r\n')}`;

    if (this.wheres.length)
      q += `\r\nWHERE ${this.wheres.map(clause => `(${clause.defer!.call(clause, this)})`).join(' AND ')}`;

    if (this.havings.length)
      q += `\r\nHAVING ${this.havings.map(clause => `(${clause.defer!.call(clause, this)})`).join(' AND ')}`;

    if (this.groupBys.length)
      q += `\r\nGROUP BY ${this.groupBys.map(col => this.colRef(col)).join(', ')}`

    if (this.orderBys.length)
      q += `\r\nORDER BY ${this.orderBys.map(o => `${this.colRef(o.col)} ${o.direction.toUpperCase()}`).join(', ')}`;

    if (this.limit! >= 0)
      q += `\r\nLIMIT ${Number(this.limit)}`;

    if (this.offset! >= 0)
      q += `\r\nOFFSET ${Number(this.offset)}`;

    if (this.unions.length)
      for (const union of this.unions) {
        if (union.type === 'ALL')
          q += '\r\nUNION ALL';
        else
          q += `\r\nUNION`;

        q += `\r\n${union.op.defer!.call(union, this)}`;
      }

    q += ';';
    return q;
  }
}

type QBaseAny<T, U, V extends QType> = Omit<QBase<T, U, V>, 'q'>
type Joinable = 'innerJoin' | 'leftJoin' | 'rightJoin' | 'crossJoin';
type Unionable = 'union' | 'unionAll';
type QSelectable<T> = Pick<QBaseAny<T, T, 'SELECT'>, 'select' | Joinable | 'where' | 'having' | Unionable | 'toSql' | 'groupBy' | 'orderBy' | 'limit' | 'offset'>
type QSelected<T, U> = Pick<QBaseAny<T, U, 'SELECT'>, Joinable | 'where' | 'having' | Unionable | 'toSql' | 'groupBy' | 'orderBy' | 'limit' | 'offset'>

type QWhere<T, U> = QSelected<T, U>
type QHaving<T, U> = Omit<QWhere<T, U>, 'where' | 'groupBy'>
type QUnion<T, U> = Pick<QSelected<T, U>, Unionable | 'toSql' | 'groupBy'>
type QGrouped<T, U, V extends QType> = Pick<QBaseAny<T, U, V>, 'groupBy' | 'toSql' | 'limit' | 'offset'>
type QOrdered<T, U, V extends QType> = Pick<QBaseAny<T, U, V>, 'orderBy' | 'toSql' | 'limit' | 'offset'>

type QUpdateable<T, U> = Pick<QBaseAny<T, U, 'UPDATE'>, Joinable | 'set' | 'groupBy'>
type QUpdated<T, U> = Pick<QBaseAny<T, U, 'UPDATE'>, 'where' | 'toSql' | 'groupBy'>

type QDeletable<T, U> = Pick<QBaseAny<T, U, 'DELETE'>, Joinable | 'tables' | 'where' | 'toSql' | 'groupBy'>

type QJoined<T, U, V extends QType> = V extends 'SELECT' ? QSelectable<T> : V extends 'UPDATE' ? QUpdateable<T, U> : V extends 'DELETE' ? QDeletable<T, U> : any;

type QColTuple<T> = [QCol<T>, T];
type ValidTuples = QColTuple<string> | QColTuple<number> | QCol<boolean> | [QCol<number>, QCol<number>] | [QCol<string>, QCol<string>] | [QCol<boolean>, QCol<boolean>];

type OrderByArgDirectional = { direction: 'asc' | 'desc'; col: QCol<unknown>; }
export type OrderByArg = QCol<unknown> | OrderByArgDirectional | (QCol<unknown> | OrderByArgDirectional)[];

type QAll = IQueryable |
  QBaseAny<any, any, any> |
  QSelectable<any> |
  QSelected<any, any> |
  QWhere<any, any> |
  QHaving<any, any> |
  QUnion<any, any> |
  QGrouped<any, any, any> |
  QOrdered<any, any, any> |
  QJoined<any, any, any>;

type QSubquery<T = any> =
  IQueryable |
  QSelectable<T> |
  QSelected<any, T> |
  QWhere<any, T> |
  QHaving<any, T> |
  QUnion<any, T> |
  QGrouped<any, T, 'SELECT'> |
  QOrdered<any, T, 'SELECT'> |
  QJoined<any, T, 'SELECT'>;

interface IQueryable {
  q: Query;
  toSql(opts?: { ptr_var: number; ptr_table: number }): string;
}

class QBase<T, TSelected, BaseType extends QType> implements IQueryable {

  constructor(
    private model: T,
    public q: Query,
    private selected: TSelected,
    private q_type: BaseType
  ) { }

  private _join<J, TKey, U extends Record<string, QColMap<unknown>>>(
    joinType: 'INNER' | 'LEFT' | 'RIGHT' | 'CROSS',
    toJoin: Table<J> | QSubquery<J>,
    t1On: (model: T) => QCol<TKey>,
    t2On: (model: QColMap<J>) => QCol<TKey>,
    join: (tModel: T, joined: QColMap<J>) => U): QJoined<U, TSelected, BaseType> {

    let qTable: QTable<J>;

    if (toJoin instanceof QBase) {
      qTable = new QTable(toJoin.model, randomUUID());

      this.q.joins.push(
        operation((q) => {
          const qString = subquery(toJoin).defer!(q);
          return `${joinType} JOIN ${qString} ${qTable.alias} ON ${q.colRef(t1On(this.model), { direct: true })} = ${q.colRef(t2On(qTable.cols), { direct: true })}`
        })
      );

    }
    else if (toJoin instanceof Table) {
      qTable = new QTable(toJoin.model, toJoin.name);
      this.q.joins.push(operation((q) => `${joinType} JOIN ${qTable.path} ${q.tableAlias(qTable)} ON ${q.colRef(t1On(this.model), { direct: true })} = ${q.colRef(t2On(qTable.cols), { direct: true })}`));
    }
    else {
      throw new Error('toJoin must be a table or subquery/virtual table');
    }

    this.q.scope.push(qTable);
    const model = join(this.model, qTable.cols);

    return new QBase(model, this.q, this.selected, this.q_type) as QJoined<U, TSelected, BaseType>;
  }

  innerJoin<J, TKey, U extends Record<string, QColMap<unknown>>>(toJoin: QSubquery<J>, t1On: (model: T) => QCol<TKey>, t2On: (model: J) => QCol<TKey>, join: (tModel: T, joined: J) => U): QJoined<U, TSelected, BaseType>;
  innerJoin<J, TKey, U extends Record<string, QColMap<unknown>>>(toJoin: Table<J> | QSubquery<J>, t1On: (model: T) => QCol<TKey>, t2On: (model: QColMap<J>) => QCol<TKey>, join: (tModel: T, joined: QColMap<J>) => U): QJoined<U, TSelected, BaseType> {
    return this._join('INNER', toJoin, t1On, t2On, join);
  }

  rightJoin<J, TKey, U extends Record<string, QColMap<unknown>>>(toJoin: QSubquery<J>, t1On: (model: T) => QCol<TKey>, t2On: (model: J) => QCol<TKey>, join: (tModel: T, joined: J) => U): QJoined<U, TSelected, BaseType>;
  rightJoin<J, TKey, U extends Record<string, QColMap<unknown>>>(toJoin: Table<J> | QSubquery<J>, t1On: (model: T) => QCol<TKey>, t2On: (model: QColMap<J>) => QCol<TKey>, join: (tModel: T, joined: QColMap<J>) => U): QJoined<U, TSelected, BaseType> {
    return this._join('RIGHT', toJoin, t1On, t2On, join);
  }

  leftJoin<J, TKey, U extends Record<string, QColMap<unknown>>>(toJoin: QSubquery<J>, t1On: (model: T) => QCol<TKey>, t2On: (model: J) => QCol<TKey>, join: (tModel: T, joined: J) => U): QJoined<U, TSelected, BaseType>;
  leftJoin<J, TKey, U extends Record<string, QColMap<unknown>>>(toJoin: Table<J> | QSubquery<J>, t1On: (model: T) => QCol<TKey>, t2On: (model: QColMap<J>) => QCol<TKey>, join: (tModel: T, joined: QColMap<J>) => U): QJoined<U, TSelected, BaseType> {
    return this._join('LEFT', toJoin, t1On, t2On, join);
  }

  crossJoin<J, TKey, U extends Record<string, QColMap<unknown>>>(toJoin: QSubquery<J>, t1On: (model: T) => QCol<TKey>, t2On: (model: J) => QCol<TKey>, join: (tModel: T, joined: J) => U): QJoined<U, TSelected, BaseType>;
  crossJoin<J, TKey, U extends Record<string, QColMap<unknown>>>(toJoin: Table<J> | QSubquery<J>, t1On: (model: T) => QCol<TKey>, t2On: (model: QColMap<J>) => QCol<TKey>, join: (tModel: T, joined: QColMap<J>) => U): QJoined<U, TSelected, BaseType> {
    return this._join('CROSS', toJoin, t1On, t2On, join);
  }

  select<U extends Record<string, QCol<unknown>>>(fn: (model: T) => U): QSelected<T, U> {
    const selected = fn(this.model);
    this.q.selected = selected;
    return new QBase(this.model, this.q, selected, this.q_type);
  }

  set(fn: (model: T) => ValidTuples[]): QUpdated<T, T> {

    const tuples = fn(this.model) as [QCol<unknown>, ExpressionArg<unknown>][];

    for (const pair of tuples)
      this.q.updates.push([pair[0], toCol(this.q, pair[1])]);

    return new QBase(this.model, this.q, this.model, this.q_type);
  }

  where(fn: (model: TSelected, data: T) => (context: AccessContext) => QCol<unknown>): QWhere<T, TSelected> {
    const evaluator = fn(this.selected, this.model);

    this.q.wheres.push(evaluator(AccessContext.Where));
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

    return new QBase(this.model, this.q, this.model, this.q_type);
  }

  having(fn: (model: TSelected, data: T) => (context: AccessContext) => QCol<unknown>): QHaving<T, TSelected> {
    const evaluator = fn(this.selected, this.model);
    this.q.havings.push(evaluator(AccessContext.Having));
    return this;
  }

  union(q2: QSelected<unknown, TSelected>) {
    const op = subquery<void>(q2 as unknown as IQueryable)
    this.q.unions.push({ op });
    return new QBase(this.model, this.q, this.model, this.q_type) as unknown as QUnion<T, TSelected>;
  }

  unionAll(q2: QSelected<unknown, TSelected>) {
    const op = subquery<void>(q2 as unknown as IQueryable)
    this.q.unions.push({ type: 'ALL', op });
    return new QBase(this.model, this.q, this.model, this.q_type) as unknown as QUnion<T, TSelected>;
  }

  groupBy(fn: (model: TSelected, data: T) => QCol<unknown> | QCol<unknown>[]): QGrouped<T, TSelected, BaseType> {
    const obj = fn(this.selected, this.model);

    if (obj instanceof QCol)
      this.q.groupBys.push(obj);
    else if (Array.isArray(obj))
      this.q.groupBys.push(...obj);
    else
      throw new Error(`groupBy predicate function must be QCol or QCol[]`);

    return new QBase(this.model, this.q, this.selected, this.q_type);
  }

  orderBy(fn: (model: TSelected, data: T) => OrderByArg) {
    const obj = fn(this.selected, this.model);

    if (obj instanceof QCol)
      this.q.orderBys.push({ col: obj, direction: 'asc' });
    else if (Array.isArray(obj)) {
      this.q.orderBys.push(...obj.map(o => {
        if (o instanceof QCol)
          return { direction: 'asc', col: o } as OrderByArgDirectional;
        else
          return o;
      }))
    }
    else if (typeof obj === 'object')
      this.q.orderBys.push(obj);
    else
      throw new Error(`orderBy predicate function must be QCol or QCol[]`);

    return this as QOrdered<T, TSelected, BaseType>

  }

  limit(n: number) {
    this.q.limit = n;
    return this as any as QSelectable<T>;
  }

  offset(n: number) {
    this.q.limit = n;
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

  values(...values: (QExpressionMap<Partial<T>> | QSelected<unknown, QColMap<T>>)[]) {

    const modelKeys = Object.keys(this.model);

    for (const value of values) {

      let col: QCol<unknown>;

      if (value instanceof QBase) {

        if (!this.q.insert.cols.length) {
          for (const key in value.q.selected) {
            if (!modelKeys.includes(key))
              throw new Error(`${key} does not exist on model ${Object.getPrototypeOf(this.model).constructor.name}`);

            this.q.insert.cols.push(key);
          }
        }

        const orderedCols = Object.fromEntries(
          this.q.insert.cols.map(key => {
            let val = value.q.selected[key];

            if (val === null || val === undefined)
              throw new Error(`Key ${key} does not exist for insert columns: [${Object.keys(value).join(', ')}]`);

            return [key, val];
          })
        );

        value.q.selected = orderedCols;

        col = subquery(value);

      } else {

        if (!this.q.insert.cols.length) {
          for (const key in value) {
            if (!modelKeys.includes(key))
              throw new Error(`${key} does not exist on model ${Object.getPrototypeOf(this.model).constructor.name}`);

            this.q.insert.cols.push(key);
          }
        }

        col = new QCol<unknown>('', undefined, (q) => this.q.insert.cols.map((key) => {

          const val = (value as Record<string, any>)[key] as ExpressionArg<unknown>;

          if (val === null || val === undefined)
            throw new Error(`Key ${key} does not exist for insert columns: [${Object.keys(value).join(', ')}]`);

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

export function from<T>(table: Table<T>): QSelectable<QColMap<T>> {
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
  q.deletes = [qTable];
  return new QBase(qTable.cols, q, qTable.cols, 'DELETE');
}

// #region Functions
export function raw<T = number | string | boolean>(value: T) {
  return operation<typeof value>((q) => `${q.colRef(toCol(q, value))}`);
}

export function count(value: QCol<unknown>) {
  return operation<number>((q) => `COUNT(${q.colRef(value)})`);
}

// #endregion

export function dbNull() {
  return new QCol<string>('NULL');
}

// #region Equality Operations

function _equalityOp<T>(target: ExpressionArg<T>, value: ExpressionArg<T>, symbol: '=' | '<>' | '<=' | '>=' | '<' | '>') {
  return operation<boolean>((q) => `${q.colRef(toCol(q, target))} ${symbol} ${q.colRef(toCol(q, value))}`);
}

export function isEqualTo<T>(target: ExpressionArg<T>, value: ExpressionArg<T>) {
  return (context: AccessContext) => operation2((q) => {
    switch (context) {
      case AccessContext.Where:
        return `${q.colRef(target, { direct: true })} = ${q.colRef(value, { direct: true })}`;
      default:
        return `${q.colRef(target)} = ${q.colRef(value)}`;
    }
  })
}

export function isGreaterThanOrEqualTo<T>(target: ExpressionArg<T>, value: ExpressionArg<T>) {
  return _equalityOp(target, value, '>=');
}

export function isLessThanOrEqualTo<T>(target: ExpressionArg<T>, value: ExpressionArg<T>) {
  return _equalityOp(target, value, '<=');
}

export function isNotEqualTo<T>(target: ExpressionArg<T>, value: ExpressionArg<T>) {
  return _equalityOp(target, value, '<>');
}

export function isGreaterThan<T>(target: ExpressionArg<T>, value: ExpressionArg<T>) {
  return _equalityOp(target, value, '>');
}

export function isLessThan<T>(target: ExpressionArg<T>, value: ExpressionArg<T>) {
  return _equalityOp(target, value, '<');
}

//#endregion

//#region Artihmatic Operations

function _arithmaticOp(target: ExpressionArg<number>, value: ExpressionArg<number>, symbol: '+' | '-' | '/' | '*' | '%') {
  return operation<number>((q) => `(${q.colRef(toCol(q, target))} ${symbol} ${q.colRef(toCol(q, value))})`);
}

export function add(target: ExpressionArg<number>, value: ExpressionArg<number>) {
  return _arithmaticOp(target, value, '+');
}

export function subtract(target: ExpressionArg<number>, value: ExpressionArg<number>) {
  return _arithmaticOp(target, value, '-');
}

export function divide(target: ExpressionArg<number>, value: ExpressionArg<number>) {
  return _arithmaticOp(target, value, '/');
}

export function multiply(target: ExpressionArg<number>, value: ExpressionArg<number>) {
  return _arithmaticOp(target, value, '*');
}

export function modulo(target: ExpressionArg<number>, value: ExpressionArg<number>) {
  return _arithmaticOp(target, value, '%');
}

export function abs(value: ExpressionArg<number>) {
  return operation<number>((q) => `ABS(${q.colRef(toCol(q, value))})`);
}

//#endregion

//#region Date Operations

export function timestamp(value: ExpressionArg<string> | Date) {
  return operation<string>((q) => {

    if (value instanceof Date)
      return `TIMESTAMP(${q.paramaterize(value.toISOString())})`;

    else
      return `TIMESTAMP(${q.colRef(toCol(q, value))})`;

  });
}

//#endregion

export function isNot(target: ExpressionArg<unknown>, value: ExpressionArg<unknown> | null) {
  return operation<boolean>((q) => {

    if (value === null)
      return `${q.colRef(toCol(q, target))} IS NOT NULL`;

    else
      return `${q.colRef(toCol(q, target))} IS NOT ${q.colRef(toCol(q, value))}`;
  })
}

export function ifNull<T>(target: ExpressionArg<unknown>, value: ExpressionArg<T>) {
  return operation<T>((q) => `IFNULL(${q.colRef(toCol(q, target))}, ${q.colRef(toCol(q, value))})`);
}

export function and(...args: QCol<boolean>[]) {
  return operation<boolean>((q) => `(${args.map(op => q.colRef(op)).join(' AND ')})`);
}

export function or(...args: QCol<boolean>[]) {
  return operation<boolean>((q) => `(${args.map(op => q.colRef(op)).join(' OR ')})`);
}

type MatchSearchModifiers = 'IN NATURAL LANGUAGE MODE' | 'IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION' | 'IN BOOLEAN MODE' | 'WITH QUERY EXPANSION';

export function match(cols: QCol<string>[], opts: { against: ExpressionArg<string>; in?: MatchSearchModifiers & string }) {
  return operation<number>((q) => {

    let against = q.colRef(toCol(q, opts.against));

    if (opts.in)
      against += ` ${opts.in}`;

    return `MATCH (${cols.map(col => q.colRef(col)).join(', ')}) AGAINST (${against})`;
  })
}


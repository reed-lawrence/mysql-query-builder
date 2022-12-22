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

type QColMapToNative<T> = { [Key in keyof T]: T[Key] extends Col<infer U> ? U : never };

export type SelectResult<T> = (QColMapToNative<GetInnerType<T>> & Omit<RowDataPacket, 'constructor'>)[];

type Arg<T = unknown> = Col<T> | T;

export type booleanish = boolean | 0 | 1;

export class Table<T> {
  public model: T;
  constructor(
    public name: string,
    public ctor: new () => T
  ) {
    this.model = new this.ctor();
  }
}

export interface IQColArgs<ColType = any, TParent extends object = any> {
  path?: string;
  parent?: QTable<TParent>;
  defer?: (q: Query, context: AccessContext) => string;
  default?: ColType;
}

export class Col<ColType = any, TParent extends object = any> {

  constructor(args: IQColArgs) {
    if (!args)
      throw new Error('No arguments provided to QCol<T>.constructor()');

    this.path = args?.path || '';
    this.parent = args?.parent;
    this.defer = args?.defer;
    this.default = args?.default;
  }

  id = randomUUID();

  path: string;

  parent?: IQColArgs<ColType, TParent>['parent'];

  defer?: IQColArgs<ColType, TParent>['defer'];

  default?: IQColArgs<ColType, TParent>['default'];;

}

interface SymbolRef {
  description: string;
  equals(o: any): boolean;
}

export const NULL: SymbolRef = {
  description: 'NULL',
  equals(o) {
    return o === this || o === null || o.description === this.description;
  }
};

export const UNKNOWN: SymbolRef = {
  description: 'UNKNOWN',
  equals(o) {
    return o === this || o.description === this.description;
  }
};

export enum AccessContext {
  Default,
  Select,
  Insert,
  Update,
  Where,
  Having,
  JoinOn,
  GroupBy,
  OrderBy
}

function operation<T>(fn: NonNullable<Col<T>['defer']>) {
  return new Col<T>({ defer: fn });
}

export function subquery<T>(value: QSubquery<T>) {
  return new Col<T>({
    defer: (q) => {

      const output = value.toSql({
        ptr_var: q.ptr_var,
        ptr_table: q.ptr_table
      });

      q.ptr_var = (value as IQueryable).q.ptr_var;
      q.ptr_table = (value as IQueryable).q.ptr_table;

      return `(${output.replace(/;/, '')})`;

    }
  });
}

export type QColMap<T> = { [Index in keyof T]: Col<T[Index]> }
type QExpressionMap<T> = { [Index in keyof T]: Col<T[Index]> | T[Index] }

class QTable<T extends object = any> {

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
      this.cols = Object.keys(base).reduce((prev, key) => {
        prev[key as keyof QColMap<T>] = new Col({ path: key, parent: this });
        return prev;
      }, {} as QColMap<T>)


  }
}

type QType = 'SELECT' | 'INSERT' | 'UPDATE' | 'DELETE';

export class Query {

  constructor(
    private from: QTable,
    private type: QType
  ) {
    this.scope = [from];
  }

  ptr_var = 1;
  ptr_table = 1;

  aliases = new Map<string, string>();
  col_store = new Map<string, { path: string; alias: string; }>();

  scope: QTable[] = [];
  selected: Record<string, Col> = {};
  wheres: Col[] = [];
  havings: Col[] = [];
  joins: Col[] = [];
  unions: { type?: 'ALL', op: Col }[] = [];
  groupBys: Col[] = [];
  orderBys: OrderByArgDirectional[] = [];

  limit?: number;
  offset?: number;

  insert = {
    cols: [] as string[],
    values: [] as Col[]
  }

  updates: [Col, Col][] = [];

  deletes: QTable[] = [];

  tableAlias(table: QTable) {
    let alias = this.aliases.get(table.id);

    if (!alias) {
      alias = table.alias || `T${this.ptr_table++}`;
      table.alias = alias;
      this.aliases.set(table.id, table.alias);
    }

    return table.alias;
  }

  toCol<T>(obj: Arg<T>) {
    if (obj instanceof Col)
      return obj;
    else if (obj === true)
      return new Col<boolean>({ path: 'TRUE' });
    else if (obj === false)
      return new Col<boolean>({ path: 'FALSE' });
    else if (typeof obj === 'string' || typeof obj === 'number')
      return new Col<T>({ path: this.paramaterize(obj) })
    else
      throw new Error(`Cannot convert ${JSON.stringify(obj)} to QCol`);
  }

  colRef<T = unknown>(arg: Col<T> | Arg<T>, context: AccessContext = AccessContext.Default) {

    const col = arg instanceof Col ? arg : this.toCol(arg);

    let stored = this.col_store.get(col.id);

    if (!stored) {
      let { path } = col;

      if (col.defer)
        path = col.defer(this, context);
      else if (col.parent)
        path = `${this.tableAlias(col.parent)}.${col.path}`;

      stored = { path, alias: '' };

      this.col_store.set(col.id, stored);

    }

    if (context === AccessContext.Where || context === AccessContext.GroupBy || context === AccessContext.Default)
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
          path = col.defer(this, AccessContext.Select);
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
    q += `\r\n${this.insert.values.map(col => `(${col.defer?.call(col, this, AccessContext.Default) || ''})`).join('\r\n')}`;
    return q;
  }

  private updateStr({ from = this.from } = {}) {
    let q = `UPDATE ${from.path} ${this.tableAlias(from)}`;
    q += `\r\nSET ${this.updates.map(pair => `${this.colRef(pair[0])}=${this.colRef(pair[1])}`).join(', ')}`;
    return q;
  }

  private deleteStr({ from = this.from } = {}) {
    let q = `DELETE ${this.deletes.map(table => this.tableAlias(table)).join(', ')} FROM ${from.path} ${this.tableAlias(from)}`;
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
      q += `\r\n${this.joins.map(join => `${join.defer!(this, AccessContext.JoinOn)}`)
        .join('\r\n')}`;

    if (this.wheres.length)
      q += `\r\nWHERE ${this.wheres.map(clause => `(${clause.defer!(this, AccessContext.Where)})`).join(' AND ')}`;

    if (this.havings.length)
      q += `\r\nHAVING ${this.havings.map(clause => `(${clause.defer!(this, AccessContext.Having)})`).join(' AND ')}`;

    if (this.groupBys.length)
      q += `\r\nGROUP BY ${this.groupBys.map(col => this.colRef(col, AccessContext.GroupBy)).join(', ')}`

    if (this.orderBys.length)
      q += `\r\nORDER BY ${this.orderBys.map(o => `${this.colRef(o.col, AccessContext.OrderBy)} ${o.direction.toUpperCase()}`).join(', ')}`;

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

        q += `\r\n${union.op.defer!(this, AccessContext.Default)}`;
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

type QColTuple<T> = [Col<T>, T];
type ValidTuples = QColTuple<string> | QColTuple<number> | Col<boolean> | [Col<number>, Col<number>] | [Col<string>, Col<string>] | [Col<boolean>, Col<boolean>];

type OrderByArgDirectional = { direction: 'asc' | 'desc'; col: Col; }
export type OrderByArg = Col | OrderByArgDirectional | (Col | OrderByArgDirectional)[];

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

  private _join<J extends object, TKey, U extends Record<string, QColMap<unknown>>>(
    joinType: 'INNER' | 'LEFT' | 'RIGHT' | 'CROSS',
    toJoin: Table<J> | QSubquery<J>,
    t1On: (model: T) => Col<TKey>,
    t2On: (model: QColMap<J>) => Col<TKey>,
    join: (tModel: T, joined: QColMap<J>) => U): QJoined<U, TSelected, BaseType> {

    let qTable: QTable<J>;

    if (toJoin instanceof QBase) {
      qTable = new QTable(toJoin.model, randomUUID());

      this.q.joins.push(
        operation((q, ctx) => {
          const qString = subquery(toJoin).defer!(q, ctx);
          return `${joinType} JOIN ${qString} ${qTable.alias} ON ${q.colRef(t1On(this.model), ctx)} = ${q.colRef(t2On(qTable.cols), ctx)}`
        })
      );

    }
    else if (toJoin instanceof Table) {
      qTable = new QTable(toJoin.model, toJoin.name);
      this.q.joins.push(operation((q) => `${joinType} JOIN ${qTable.path} ${q.tableAlias(qTable)} ON ${q.colRef(t1On(this.model), AccessContext.JoinOn)} = ${q.colRef(t2On(qTable.cols), AccessContext.JoinOn)}`));
    }
    else {
      throw new Error('toJoin must be a table or subquery/virtual table');
    }

    this.q.scope.push(qTable);
    const model = join(this.model, qTable.cols);

    return new QBase(model, this.q, this.selected, this.q_type) as QJoined<U, TSelected, BaseType>;
  }

  innerJoin<J extends object, TKey, U extends Record<string, QColMap<unknown>>>(toJoin: QSubquery<J>, t1On: (model: T) => Col<TKey>, t2On: (model: J) => Col<TKey>, join: (tModel: T, joined: J) => U): QJoined<U, TSelected, BaseType>;
  innerJoin<J extends object, TKey, U extends Record<string, QColMap<unknown>>>(toJoin: Table<J> | QSubquery<J>, t1On: (model: T) => Col<TKey>, t2On: (model: QColMap<J>) => Col<TKey>, join: (tModel: T, joined: QColMap<J>) => U): QJoined<U, TSelected, BaseType> {
    return this._join('INNER', toJoin, t1On, t2On, join);
  }

  rightJoin<J extends object, TKey, U extends Record<string, QColMap<unknown>>>(toJoin: QSubquery<J>, t1On: (model: T) => Col<TKey>, t2On: (model: J) => Col<TKey>, join: (tModel: T, joined: J) => U): QJoined<U, TSelected, BaseType>;
  rightJoin<J extends object, TKey, U extends Record<string, QColMap<unknown>>>(toJoin: Table<J> | QSubquery<J>, t1On: (model: T) => Col<TKey>, t2On: (model: QColMap<J>) => Col<TKey>, join: (tModel: T, joined: QColMap<J>) => U): QJoined<U, TSelected, BaseType> {
    return this._join('RIGHT', toJoin, t1On, t2On, join);
  }

  leftJoin<J extends object, TKey, U extends Record<string, QColMap<unknown>>>(toJoin: QSubquery<J>, t1On: (model: T) => Col<TKey>, t2On: (model: J) => Col<TKey>, join: (tModel: T, joined: J) => U): QJoined<U, TSelected, BaseType>;
  leftJoin<J extends object, TKey, U extends Record<string, QColMap<unknown>>>(toJoin: Table<J> | QSubquery<J>, t1On: (model: T) => Col<TKey>, t2On: (model: QColMap<J>) => Col<TKey>, join: (tModel: T, joined: QColMap<J>) => U): QJoined<U, TSelected, BaseType> {
    return this._join('LEFT', toJoin, t1On, t2On, join);
  }

  crossJoin<J extends object, TKey, U extends Record<string, QColMap<unknown>>>(toJoin: QSubquery<J>, t1On: (model: T) => Col<TKey>, t2On: (model: J) => Col<TKey>, join: (tModel: T, joined: J) => U): QJoined<U, TSelected, BaseType>;
  crossJoin<J extends object, TKey, U extends Record<string, QColMap<unknown>>>(toJoin: Table<J> | QSubquery<J>, t1On: (model: T) => Col<TKey>, t2On: (model: QColMap<J>) => Col<TKey>, join: (tModel: T, joined: QColMap<J>) => U): QJoined<U, TSelected, BaseType> {
    return this._join('CROSS', toJoin, t1On, t2On, join);
  }

  select<U extends Record<string, Col>>(fn: (model: T) => U): QSelected<T, U> {
    const selected = fn(this.model);
    this.q.selected = selected;
    return new QBase(this.model, this.q, selected, this.q_type);
  }

  set(fn: (model: T) => ValidTuples[]): QUpdated<T, T> {

    const tuples = fn(this.model) as [Col, Arg<unknown>][];

    for (const pair of tuples)
      this.q.updates.push([pair[0], this.q.toCol(pair[1])]);

    return new QBase(this.model, this.q, this.model, this.q_type);
  }

  where(fn: (model: TSelected, data: T) => Col): QWhere<T, TSelected> {
    this.q.wheres.push(fn(this.selected, this.model));
    return this;
  }

  tables(...tables: Table<unknown>[]): QDeletable<T, T> {

    const deletes: QTable[] = [];

    for (const table of tables) {
      const toDelete = this.q.scope.find(t => t.base instanceof table.ctor);
      if (!toDelete)
        throw new Error(`Table ${table.name} not registered in query scope`);
      deletes.push(toDelete);
    }

    this.q.deletes = deletes;

    return new QBase(this.model, this.q, this.model, this.q_type);
  }

  having(fn: (model: TSelected, data: T) => Col): QHaving<T, TSelected> {
    this.q.havings.push(fn(this.selected, this.model));
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

  groupBy(fn: (model: TSelected, data: T) => Col | Col[]): QGrouped<T, TSelected, BaseType> {
    const obj = fn(this.selected, this.model);

    if (obj instanceof Col)
      this.q.groupBys.push(obj);
    else if (Array.isArray(obj))
      this.q.groupBys.push(...obj);
    else
      throw new Error(`groupBy predicate function must be QCol or QCol[]`);

    return new QBase(this.model, this.q, this.selected, this.q_type);
  }

  orderBy(fn: (model: TSelected, data: T) => OrderByArg) {
    const obj = fn(this.selected, this.model);

    if (obj instanceof Col)
      this.q.orderBys.push({ col: obj, direction: 'asc' });
    else if (Array.isArray(obj)) {
      this.q.orderBys.push(...obj.map(o => {
        if (o instanceof Col)
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

      let col: Col;

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

        col = new Col({
          defer: (q, ctx) => this.q.insert.cols.map((key) => {

            const val = (value as Record<string, any>)[key] as Arg<unknown>;

            if (val === null || val === undefined)
              throw new Error(`Key ${key} does not exist for insert columns: [${Object.keys(value).join(', ')}]`);

            return q.colRef(val, ctx);

          }).join(', ')
        })

      }

      this.q.insert.values.push(col);

    }

    return this;
  }

  toSql(opts?: { ptr_var: number; ptr_table: number }) {
    return this.q.toSql(opts);
  }
}

export function from<T extends object>(table: Table<T>): QSelectable<QColMap<T>> {
  const qTable = new QTable(table.model, table.name);
  const q = new Query(qTable, 'SELECT');
  q.selected = qTable.cols;
  return new QBase(qTable.cols, q, qTable.cols, 'SELECT');
}

export function insertInto<T extends object>(table: Table<T>) {
  const qTable = new QTable(table.model, table.name);
  const q = new Query(qTable, 'INSERT');
  return new QInsertable(qTable.cols, q);
}

export function update<T extends object>(table: Table<T>): QUpdateable<QColMap<T>, QColMap<T>> {
  const qTable = new QTable(table.model, table.name);
  const q = new Query(qTable, 'UPDATE');
  return new QBase(qTable.cols, q, qTable.cols, 'UPDATE');
}

export function deleteFrom<T extends object>(table: Table<T>): QDeletable<QColMap<T>, QColMap<T>> {
  const qTable = new QTable(table.model, table.name);
  const q = new Query(qTable, 'DELETE');
  q.deletes = [qTable];
  return new QBase(qTable.cols, q, qTable.cols, 'DELETE');
}

// #region Functions
export function raw<T = number | string | boolean>(value: T) {
  return operation<typeof value>((q, ctx) => `${q.colRef(value, ctx)}`);
}

export function count(value: Col) {
  return operation<number>((q, ctx) => `COUNT(${q.colRef(value, ctx)})`);
}

// #endregion

// #region Equality Operations

function _equalityOp<T>(target: Arg<T>, value: Arg<T>, symbol: '=' | '<>' | '<=' | '>=' | '<' | '>' | '<=>') {
  return new Col<boolean>({ defer: (q, ctx) => `${q.colRef(target, ctx)} ${symbol} ${q.colRef(value, ctx)}` });
}

export function equalTo<T>(target: Arg<T>, value: Arg<T>, args?: { null_safe: boolean }): Col<boolean>;
export function equalTo(target: Arg<boolean | 1 | 0>, value: Arg<boolean | 1 | 0>, args?: { null_safe: boolean }): Col<boolean>;
export function equalTo<T>(target: Arg<T>, value: Arg<T>, args?: { null_safe: boolean }): Col<boolean> {
  return _equalityOp(target, value, args?.null_safe ? '<=>' : '=');
}

export function notEqualTo<T>(target: Arg<T>, value: Arg<T>) {
  return _equalityOp(target, value, '<>');
}

export function greaterThan<T>(target: Arg<T>, value: Arg<T>) {
  return _equalityOp(target, value, '>');
}

export function greaterThanOrEqualTo<T>(target: Arg<T>, value: Arg<T>) {
  return _equalityOp(target, value, '>=');
}

export function lessThan<T>(target: Arg<T>, value: Arg<T>) {
  return _equalityOp(target, value, '<');
}

export function lessThanOrEqualTo<T>(target: Arg<T>, value: Arg<T>) {
  return _equalityOp(target, value, '<=');
}

//#endregion

//#region Artihmatic Operations

function _arithmaticOp(target: Arg<number>, value: Arg<number>, symbol: '+' | '-' | '/' | '*' | '%' | 'DIV') {
  return new Col<number>({ defer: (q, ctx) => `(${q.colRef(target, ctx)} ${symbol} ${q.colRef(value, ctx)})` });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/arithmetic-functions.html#operator_plus
 * 
 * ```SQL
 * mysql> SELECT 3 + 5;
 *         -> 8
 * ```
 */
export function add(target: Arg<number>, value: Arg<number>) {
  return _arithmaticOp(target, value, '+');
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/arithmetic-functions.html#operator_minus
 * 
 * ```SQL
 * mysql> SELECT 3 - 5;
 *         -> -2
 * ```
 */
export function subtract(target: Arg<number>, value: Arg<number>) {
  return _arithmaticOp(target, value, '-');
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/arithmetic-functions.html#operator_minus
 * 
 * ```SQL
 * mysql> SELECT 3 - 5;
 *         -> -2
 * ```
 */
export const minus = subtract;

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/arithmetic-functions.html#operator_unary-minus
 * 
 * Unary minus. This operator changes the sign of the operand.
 * 
 * ```SQL
 * mysql> SELECT - 2;
 *         -> -2
 * ```
 */
export function unary_minus(target: Arg<number>): Col<number> {
  return new Col({
    defer(q, context) {
      return `- ${q.colRef(target, context)}`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/arithmetic-functions.html#operator_divide
 * 
 * ```SQL
 * mysql> SELECT 3/5;
 *         -> 0.60
 * ```
 * 
 * Division by zero produces a `NULL` result:
 * ```SQL
 * mysql> SELECT 102/(1-1);
 *         -> NULL
 * ```
 * 
 * A division is calculated with `BIGINT` arithmetic only if performed in a context where its 
 * result is converted to an integer.
 */
export function divide(target: Arg<number>, value: Arg<number>): Col<number>;

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/arithmetic-functions.html#operator_divide
 * 
 * Integer division. Discards from the division result any fractional part to the right of the decimal point.
 * 
 * If either operand has a noninteger type, the operands are converted to `DECIMAL` and divided using `DECIMAL` 
 * arithmetic before converting the result to `BIGINT`. If the result exceeds `BIGINT` range, an error occurs.
 * 
 * ```SQL
 * mysql> SELECT 5 DIV 2, -5 DIV 2, 5 DIV -2, -5 DIV -2;
 *         -> 2, -2, -2, 2
 * ```
 */
export function divide(target: Arg<number>, value: Arg<number>, args: { integer: true }): Col<number>;
export function divide(target: Arg<number>, value: Arg<number>, args?: { integer: boolean }) {
  return _arithmaticOp(target, value, args?.integer ? 'DIV' : '/');
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/arithmetic-functions.html#operator_times
 * 
 * ```SQL
 * mysql> SELECT 3 * 5;
 *         -> 15
 * mysql> SELECT 18014398509481984 * 18014398509481984.0;
 *         -> 324518553658426726783156020576256.0
 * mysql> SELECT 18014398509481984 * 18014398509481984;
 *         --> out-of-range error
 * ```
 * 
 * The last expression produces an error because the result of the integer multiplication exceeds 
 * the 64-bit range of `BIGINT` calculations. 
 * [(See Section 11.1, “Numeric Data Types”.)](https://dev.mysql.com/doc/refman/8.0/en/numeric-types.html)
 */
export function multiply(target: Arg<number>, value: Arg<number>) {
  return _arithmaticOp(target, value, '*');
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/arithmetic-functions.html#operator_times
 * 
 * ```SQL
 * mysql> SELECT 3 * 5;
 *         -> 15
 * mysql> SELECT 18014398509481984 * 18014398509481984.0;
 *         -> 324518553658426726783156020576256.0
 * mysql> SELECT 18014398509481984 * 18014398509481984;
 *         --> out-of-range error
 * ```
 * 
 * The last expression produces an error because the result of the integer multiplication exceeds 
 * the 64-bit range of `BIGINT` calculations. 
 * [(See Section 11.1, “Numeric Data Types”.)](https://dev.mysql.com/doc/refman/8.0/en/numeric-types.html)
 */
export const times = multiply;


/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/arithmetic-functions.html#operator_mod
 * 
 * ```SQL
 * mysql> SELECT 234 % 10;
 *         -> 4
 * mysql> SELECT 253 % 7;
 *         -> 1
 * mysql> SELECT 29 % 9;
 *         -> 2
 * mysql> SELECT 29 % 9;
 *         -> 2
 * ```
 * 
 * Modulo operation. Returns the remainder of `N` divided by `M`. For more information, see the 
 * description for the `MOD()` function in 
 * [Section 12.6.2, “Mathematical Functions”](https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html).
 * 
 * See also: 
 * - {@link mod}
 */
export function modulo(target: Arg<number>, value: Arg<number>) {
  return _arithmaticOp(target, value, '%');
}

//#endregion

// #region MATHEMATICAL FUNCTIONS

/**
 * Returns the absolute value of `X`, or `NULL` if `X` is `NULL`.
 * 
 * The result type is derived from the argument type. An implication of this is that 
 * `ABS(-9223372036854775808)` produces an error because the result cannot be stored 
 * in a signed `BIGINT` value.
 * 
 * ```SQL
 * mysql> SELECT ABS(2);
 *         -> 2
 * mysql> SELECT ABS(-32);
 *         -> 32
 * ```
 * 
 * This function is safe to use with `BIGINT` values.
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_abs
 */
export function abs(x: Arg<number>) {
  return new Col<number>({ defer: (q, ctx) => `ABS(${q.colRef(x, ctx)})` });
}

/**
 * Returns the arc cosine of `X`, that is, the value whose cosine is `X`. Returns `NULL` 
 * if `X` is not in the range `-1` to `1`, or if `X` is `NULL`.
 * 
 * ```SQL
 * mysql> SELECT ACOS(1);
 *         -> 0
 * mysql> SELECT ACOS(1.0001);
 *         -> NULL
 * mysql> SELECT ACOS(0);
 *         -> 1.5707963267949
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_acos
 */
export function acos(x: Arg<number>) {
  return new Col<number>({ defer: (q, ctx) => `ACOS(${q.colRef(x, ctx)})` });
}

/**
 * Returns the arc sine of `X`, that is, the value whose sine is `X`. Returns `NULL` if 
 * `X` is not in the range `-1` to `1`, or if `X` is `NULL`.
 * 
 * ```SQL
 * mysql> SELECT ASIN(0.2);
 *         -> 0.20135792079033
 * mysql> SELECT ASIN('foo');
 *         -> 0 -- Warning (1292): Truncated incorrect DOUBLE value: 'foo'
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_asin
 */
export function asin(x: Arg<number>) {
  return new Col<number>({ defer: (q, ctx) => `ASIN(${q.colRef(x, ctx)})` });
}

export function atan(value: Arg<number>, val2?: Arg<number>) {
  if (val2)
    return new Col<number>({ defer: (q, ctx) => `ATAN(${q.colRef(value, ctx)}, ${q.colRef(val2, ctx)})` });
  else
    return new Col<number>({ defer: (q, ctx) => `ATAN(${q.colRef(value, ctx)})` });
}

export function atan2(val1: Arg<number>, val2: Arg<number>) {
  return new Col<number>({ defer: (q, ctx) => `ATAN2(${q.colRef(val1, ctx)}, ${q.colRef(val2, ctx)})` });
}

export function ceiling(value: Arg<number>) {
  return new Col<number>({ defer: (q, ctx) => `CEILING(${q.colRef(value, ctx)})` });
}

export function conv(value: Arg<number | string>, from: Arg<number>, to: Arg<number>) {
  return new Col<number>({ defer: (q, ctx) => `CONV(${q.colRef(value, ctx)}, ${q.colRef(from, ctx)}, ${q.colRef(to, ctx)})` });
}

export function cos(value: Arg<number>) {
  return new Col<number>({ defer: (q, ctx) => `COS(${q.colRef(value, ctx)})` });
}

export function cot(value: Arg<number>) {
  return new Col<number>({ defer: (q, ctx) => `COT(${q.colRef(value, ctx)})` });
}

export function crc32(value: Arg<number>) {
  return new Col<number>({ defer: (q, ctx) => `CRC32(${q.colRef(value, ctx)})` });
}

export function degrees(value: Arg<number>) {
  return new Col<number>({ defer: (q, ctx) => `DEGREES(${q.colRef(value, ctx)})` });
}

export function exp(value: Arg<number>) {
  return new Col<number>({ defer: (q, ctx) => `EXP(${q.colRef(value, ctx)})` });
}

export function floor(value: Arg<number>) {
  return new Col<number>({ defer: (q, ctx) => `FLOOR(${q.colRef(value, ctx)})` });
}

// #endregion

// #region STRING OPERATIONS

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_ascii
 * 
 * Returns the numeric value of the leftmost character of the string `str`. Returns `0` if str is 
 * the empty string. Returns `NULL` if `str` is `NULL`. `ASCII()` works for 8-bit characters.
 * 
 * ```SQL
 * mysql> SELECT ASCII('2');
 *         -> 50
 * mysql> SELECT ASCII(2);
 *         -> 50
 * mysql> SELECT ASCII('dx');
 *         -> 100
 * ```
 * 
 * See also: 
 * - {@link ord}
 */
export function ascii(str: Arg<string>): Col<number> {
  return new Col({
    defer(q, context) {
      return `ASCII(${q.colRef(str, context)})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_bin
 * 
 * Returns a string representation of the binary value of `N`, where `N` is a longlong (`BIGINT`) 
 * number. This is equivalent to `CONV(N,10,2)`. Returns `NULL` if N is `NULL`.
 * 
 * ```SQL
 * mysql> SELECT BIN(12);
 *        -> '1100'
 * ```
 * 
 * See also: 
 * - {@link conv}
 */
export function bin(n: Arg<number>): Col<string> {
  return new Col({
    defer(q, context) {
      return `BIN(${q.colRef(n, context)})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_bit-length
 * 
 * Returns the length of the string `str` in bits. Returns `NULL` if `str` is `NULL`.
 * 
 * ```SQL
 * mysql> SELECT BIT_LENGTH('text');
 *        -> 32
 * ```
 */
export function bit_length(str: Arg<number>): Col<number> {
  return new Col({
    defer(q, context) {
      return `BIT_LENGTH(${q.colRef(str, context)})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_char
 * 
 * `CHAR()` interprets each argument `N` as an integer and returns a string consisting 
 * of the characters given by the code values of those integers. `NULL` values are skipped.
 * 
 * By default, `CHAR()` returns a binary string. To produce a string in a given character set, 
 * use the optional `USING` clause:
 * 
 * ```SQL
 * mysql> SELECT CHAR(77, 121, 83, 81, '76' USING utf8mb4);
 *        -> 'MySQL'
 * ```
 * 
 * Usage: 
 * ```typescript
 * char({ values: [77, 121, 83, 81, '76'], using: 'utf8mb4' }) // => Col<string> -> SELECT CHAR(77, 121, 83, 81, '76' USING utf8mb4)
 * ```
 * 
 * If `USING` is given and the result string is illegal for the given character set, a warning is issued. 
 * Also, if strict SQL mode is enabled, the result from `CHAR()` becomes `NULL`.
 * 
 * If `CHAR()` is invoked from within the mysql client, binary strings display using hexadecimal 
 * notation, depending on the value of the 
 * [`--binary-as-hex`](https://dev.mysql.com/doc/refman/8.0/en/mysql-command-options.html#option_mysql_binary-as-hex). 
 * For more information about that option, see 
 * [Section 4.5.1, “mysql — The MySQL Command-Line Client”](https://dev.mysql.com/doc/refman/8.0/en/mysql.html). 
 * 
 * `CHAR()` arguments larger than 255 are converted into multiple result bytes. For example, `CHAR(256) `
 * is equivalent to `CHAR(1,0)`, and `CHAR(256*256)` is equivalent to `CHAR(1,0,0)`
 */
export function char(args: { values: Arg<number | string>[], using: 'utf8mb4' & string }): Col<string>;

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_char
 * 
 * `CHAR()` interprets each argument `N` as an integer and returns a string consisting 
 * of the characters given by the code values of those integers. `NULL` values are skipped.
 * 
 * ```SQL
 * mysql> SELECT CHAR(77, 121, 83, 81, '76');
 *        -> '0x4D7953514C'
 * ```
 * 
 * If `CHAR()` is invoked from within the mysql client, binary strings display using hexadecimal 
 * notation, depending on the value of the 
 * [`--binary-as-hex`](https://dev.mysql.com/doc/refman/8.0/en/mysql-command-options.html#option_mysql_binary-as-hex). 
 * For more information about that option, see 
 * [Section 4.5.1, “mysql — The MySQL Command-Line Client”](https://dev.mysql.com/doc/refman/8.0/en/mysql.html). 
 * 
 * `CHAR()` arguments larger than 255 are converted into multiple result bytes. For example, `CHAR(256) `
 * is equivalent to `CHAR(1,0)`, and `CHAR(256*256)` is equivalent to `CHAR(1,0,0)`
 */
export function char(...integers: Arg<number | string>[]): Col<string>;
export function char(args: any) {

  let using = '';
  let values: Iterable<Arg<number | string>>;

  if (typeof args === 'object' && !(args instanceof Col) && args.using && Array.isArray(args.values)) {
    values = args.values;
    using = args.using;
  }

  else
    values = arguments;

  return new Col({
    defer(q, context) {

      const csv = Array.prototype.reduce.call(values, (out, arg) => {
        if (!arg)
          return out;

        const str = q.colRef(arg, context);
        return out ? `${out}, ${str}` : str;

      }, '') as string;

      if (using)
        return `CHAR(${csv} USING ${using})`;
      else
        return `CHAR(${csv})`;

    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_char-length
 * 
 * Returns the length of the string `str`, measured in code points. A multibyte character 
 * counts as a single code point. This means that, for a string containing two 3-byte characters,
 * `LENGTH()` returns 6, whereas `CHAR_LENGTH()` returns 2, as shown here:
 * 
 * ```SQL
 * mysql> SET @dolphin := '海豚';
 * mysql> SELECT LENGTH(@dolphin), CHAR_LENGTH(@dolphin);
 * ```
 * ```text
 * +------------------+-----------------------+
 * | LENGTH(@dolphin) | CHAR_LENGTH(@dolphin) |
 * +------------------+-----------------------+
 * |                6 |                     2 |
 * +------------------+-----------------------+
 * ```
 * 
 * `CHAR_LENGTH()` returns `NULL` if `str` is `NULL`.
 */
export function char_length(value: Arg<string>): Col<number> {
  return new Col({
    defer(q, context) {
      return `CHAR_LENGTH(${q.colRef(value, context)})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_concat
 * 
 * Returns the string that results from concatenating the arguments. May have one or 
 * more arguments. If all arguments are nonbinary strings, the result is a nonbinary 
 * string. If the arguments include any binary strings, the result is a binary string. 
 * A numeric argument is converted to its equivalent nonbinary string form.
 * 
 * `CONCAT()` returns `NULL` if any argument is `NULL`.
 * 
 * ```SQL
 * mysql> SELECT CONCAT('My', 'S', 'QL');
 *         -> 'MySQL'
 * mysql> SELECT CONCAT('My', NULL, 'QL');
 *         -> NULL
 * mysql> SELECT CONCAT(14.3);
 *         -> '14.3'
 * ```
 * 
 * If `CONCAT()` is invoked from within the mysql client, binary string results display 
 * using hexadecimal notation, depending on the value of the 
 * [--binary-as-hex](https://dev.mysql.com/doc/refman/8.0/en/mysql-command-options.html#option_mysql_binary-as-hex). 
 * 
 * For more information about that option, see 
 * [Section 4.5.1, “mysql — The MySQL Command-Line Client”](https://dev.mysql.com/doc/refman/8.0/en/mysql.html).
 */
export function concat(...args: Col<string | number>[]): Col<string> {
  return new Col({
    defer(q, context) {
      return `CONCAT(${args.reduce((out, arg) => {
        const str = q.colRef(arg, context);
        return out ? `${out}, ${str}` : str;
      }, '')})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_concat-ws
 * 
 * `CONCAT_WS()` stands for Concatenate With Separator and is a special form of `CONCAT()`. 
 * The first argument is the separator for the rest of the arguments. The separator is added 
 * between the strings to be concatenated. The separator can be a string, as can the rest of 
 * the arguments. 
 * 
 * If the separator is `NULL`, the result is `NULL`.
 * 
 * ```SQL
 * mysql> SELECT CONCAT_WS(',','First name','Second name','Last Name');
 *         -> 'First name,Second name,Last Name'
 * mysql> SELECT CONCAT_WS(',','First name',NULL,'Last Name');
 *         -> 'First name,Last Name'
 * ```
 * 
 * `CONCAT_WS()` does not skip empty strings. However, it does skip any `NULL` values after the separator argument.
 * 
 * See also: 
 * - {@link concat}
 */
export function concat_ws(separator: Col<string>, ...args: Col<string | number>[]): Col<string> {
  return new Col({
    defer(q, context) {
      return `CONCAT_WS(${q.colRef(separator, context)}, ${args.reduce((out, arg) => {
        const str = q.colRef(arg, context);
        return out ? `${out}, ${str}` : str;
      }, '')})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_elt
 * 
 * `ELT()` returns the `N`th element of the list of strings: `str1` if `N = 1`, `str2` if `N = 2`, and so on. 
 * Returns `NULL` if `N` is less than `1`, greater than the number of arguments, or `NULL`. 
 * 
 * `ELT()` is the complement of `FIELD()`.
 * 
 * ```SQL
 * mysql> SELECT ELT(1, 'Aa', 'Bb', 'Cc', 'Dd');
 *         -> 'Aa'
 * mysql> SELECT ELT(4, 'Aa', 'Bb', 'Cc', 'Dd');
 *         -> 'Dd'
 * ```
 * 
 * See also: 
 * - {@link field}
 */
export function elt(n: Col<number>, ...args: Col<string>[]): Col<string>;
export function elt(...args: Col[]): Col<string> {
  return new Col({
    defer(q, context) {
      return `ELT(${args.reduce((out, arg) => {
        const str = q.colRef(arg, context);
        return out ? `${out}, ${str}` : str;
      }, '')})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_export-set
 * 
 * Returns a string such that for every bit set in the value `bits`, you get an `on` 
 * string and for every bit not set in the value, you get an `off` string. Bits in `bits` 
 * are examined from right to left (from low-order to high-order bits). Strings are added 
 * to the result from left to right, separated by the `separator` string (the default being 
 * the comma character `,`). The number of bits examined is given by `number_of_bits`, which 
 * has a default of `64` if not specified. `number_of_bits` is silently clipped to `64` if l
 * arger than `64`. It is treated as an unsigned integer, so a value of `−1` is effectively 
 * the same as `64`.
 * 
 * ```SQL
 * mysql> SELECT EXPORT_SET(5,'Y','N',',',4);
 *         -> 'Y,N,Y,N'
 * mysql> SELECT EXPORT_SET(6,'1','0',',',10);
 *         -> '0,1,1,0,0,0,0,0,0,0'
 * ```
 */
export function export_set(bits: Arg<number>, on: Arg<string>, off: Arg<string>, separator: Arg<string>, number_of_bits: Arg<number>): Col<string>;
export function export_set(...args: Arg[]): Col<string> {
  return new Col({
    defer(q, context) {
      return `EXPORT_SET(${args.reduce((out: string, arg: Arg) => {
        const str = q.colRef(arg, context);
        return out ? `${out}, ${str}` : str;
      }, '')})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_field
 * 
 * Returns the index (position) of `str` in the `str1`, `str2`, `str3`, ... list. 
 * Returns `0` if `str` is not found.
 *
 * If all arguments to `FIELD()` are strings, all arguments are compared as strings. 
 * If all arguments are numbers, they are compared as numbers. Otherwise, the arguments 
 * are compared as double.
 * 
 * If `str` is `NULL`, the return value is `0` because `NULL` fails equality comparison 
 * with any value. `FIELD()` is the complement of `ELT()`.
 * 
 * ```SQL
 * mysql> SELECT FIELD('Bb', 'Aa', 'Bb', 'Cc', 'Dd', 'Ff');
 *         -> 2
 * mysql> SELECT FIELD('Gg', 'Aa', 'Bb', 'Cc', 'Dd', 'Ff');
 *         -> 0
 * ```
 * 
 * See also: 
 * - {@link elt}
 */
export function field(str: Col<string>, ...list: Col<string>[]): Col<number>;
export function field(...args: Col[]): Col<number> {
  return new Col({
    defer(q, context) {
      return `FIELD(${args.reduce((out, arg) => {
        const str = q.colRef(arg, context);
        return out ? `${out}, ${str}` : str;
      }, '')})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_find-in-set
 * 
 * Returns a value in the range of `1` to `N` if the string `str` is in the string list `strlist` 
 * consisting of `N` substrings. A string list is a string composed of substrings separated by `,` 
 * characters. If the first argument is a constant string and the second is a column of type `SET`,
 * the `FIND_IN_SET()` function is optimized to use bit arithmetic. Returns `0` if `str` is not in 
 * `strlist` or if `strlist` is the empty string. Returns `NULL` if either argument is `NULL`. 
 * This function does not work properly if the first argument contains a `,` character.
 * 
 * ```SQL
 * mysql> SELECT FIND_IN_SET('b','a,b,c,d');
 *         -> 2
 * ```
 */
export function find_in_set(str: Arg<string>, strlist: Arg<string>) {
  return new Col<string>({
    defer(q, context) {
      return `FIND_IN_SET(${q.colRef(str, context)}, ${q.colRef(strlist, context)})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_format
 * 
 * Formats the number `X` to a format like `'#,###,###.##'`, rounded to `D` decimal places, 
 * and returns the result as a string. If `D` is `0`, the result has no decimal point or 
 * fractional part. If `X` or `D` is `NULL`, the function returns `NULL`.
 * 
 * The optional third parameter enables a locale to be specified to be used for the result 
 * number's decimal point, thousands separator, and grouping between separators. Permissible 
 * locale values are the same as the legal values for the 
 * [`lc_time_names`](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_lc_time_names) 
 * system variable. If the locale is `NULL` or not specified, the default locale is `'en_US'`.
 * 
 * ```SQL
 * mysql> SELECT FORMAT(12332.123456, 4);
 *         -> '12,332.1235'
 * mysql> SELECT FORMAT(12332.1,4);
 *         -> '12,332.1000'
 * mysql> SELECT FORMAT(12332.2,0);
 *         -> '12,332'
 * mysql> SELECT FORMAT(12332.2,2,'de_DE');
 *         -> '12.332,20'
 * ```
 */
export function format(x: Arg<number>, d: Arg<number>, locale?: Arg<string & 'en_US'>): Col<string>;
export function format(...args: Arg[]): Col<string> {
  return new Col<string>({
    defer(q, context) {
      return `FORMAT(${args.reduce((out: string, arg: Arg) => {
        const str = q.colRef(arg, context);
        return out ? `${out}, ${str}` : str;
      }, '')})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_from-base64
 * 
 * Takes a string encoded with the base-64 encoded rules used by `TO_BASE64()` 
 * and returns the decoded result as a binary string. The result is `NULL` if the 
 * argument is `NULL` or not a valid base-64 string. See the description of `TO_BASE64()`
 * for details about the encoding and decoding rules.
 * 
 * ```SQL
 * mysql> SELECT TO_BASE64('abc'), FROM_BASE64(TO_BASE64('abc'));
          -> 'JWJj', 'abc'
 * ```
 * If `FROM_BASE64()` is invoked from within the mysql client, binary strings display using 
 * hexadecimal notation. You can disable this behavior by setting the value of the 
 * [--binary-as-hex](https://dev.mysql.com/doc/refman/8.0/en/mysql-command-options.html#option_mysql_binary-as-hex) 
 * to 0 when starting the mysql client. 
 * 
 * For more information about that option, see 
 * [Section 4.5.1, “mysql — The MySQL Command-Line Client”.](https://dev.mysql.com/doc/refman/8.0/en/mysql.html)
 * 
 * See also: 
 * - {@link to_base64}
 */
export function from_base64(str: Arg<string>) {
  return new Col<string>({
    defer(q, context) {
      return `FROM_BASE64(${q.colRef(str, context)})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_hex
 * 
 * For a string argument `str`, `HEX()` returns a hexadecimal string representation 
 * of `str` where each byte of each character in `str` is converted to two hexadecimal 
 * digits. (Multibyte characters therefore become more than two digits.) The inverse 
 * of this operation is performed by the `UNHEX()` function.
 * 
 * For a numeric argument `N`, `HEX()` returns a hexadecimal string representation of 
 * the value of `N` treated as a longlong (`BIGINT`) number. This is equivalent to 
 * `CONV(N,10,16)`. The inverse of this operation is performed by `CONV(HEX(N),16,10)`.
 * 
 * For a `NULL` argument, this function returns `NULL`.
 * 
 * ```SQL
 * mysql> SELECT X'616263', HEX('abc'), UNHEX(HEX('abc'));
 *         -> 'abc', 616263, 'abc'
 * mysql> SELECT HEX(255), CONV(HEX(255),16,10);
 *         -> 'FF', 255
 * ```
 * 
 * See also: 
 * - {@link unhex}
 * - {@link conv}
 */
export function hex(value: Arg<string | number>): Col<string> {
  return new Col({
    defer(q, context) {
      return `HEX(${q.colRef(value, context)})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_insert
 * 
 * Returns the string str, with the substring beginning at position pos and len characters 
 * long replaced by the string newstr. Returns the original string if pos is not within 
 * the length of the string. Replaces the rest of the string from position pos if len is 
 * not within the length of the rest of the string. Returns NULL if any argument is NULL.
 * 
 * ```SQL
 * mysql> SELECT INSERT('Quadratic', 3, 4, 'What');
 *        -> 'QuWhattic'
 * mysql> SELECT INSERT('Quadratic', -1, 4, 'What');
 *        -> 'Quadratic'
 * mysql> SELECT INSERT('Quadratic', 3, 100, 'What');
 *        -> 'QuWhat'
 * ```
 */
export function iinsert(value: Arg<string>, pos: Arg<number>, len: Arg<number>, str: Arg<string>) {
  return new Col<string>({
    defer(q, context) {
      return `INSERT(${q.colRef(value, context)}, ${q.colRef(pos, context)}, ${q.colRef(len, context)}, ${q.colRef(str, context)})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_instr
 * 
 * Returns the position of the first occurrence of substring substr in string str. 
 * This is the same as the two-argument form of LOCATE(), except that the order of 
 * the arguments is reversed.
 * 
 * ```SQL
 * mysql> SELECT INSTR('foobarbar', 'bar');
 *        -> 4
 * mysql> SELECT INSTR('xbar', 'foobar');
 *        -> 0
 * ```
 */
export function instr(string: Arg<string>, substr: Arg<string>) {
  return new Col<string>({
    defer(q, context) {
      return `INSTR(${q.colRef(string, context)}, ${q.colRef(substr, context)})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_lower
 * 
 * Returns the string str with all characters changed to lowercase according to the 
 * current character set mapping, or NULL if str is NULL. The default character set 
 * is utf8mb4.
 * 
 * ```SQL
 * mysql> SELECT LOWER('QUADRATICALLY');
 *        -> 'quadratically'
 * ```
 */
export function lower(value: Arg<string>) {
  return new Col<string>({
    defer(q, context) {
      return `LOWER(${q.colRef(value, context)})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_left
 * 
 * Returns the leftmost len characters from the string str, or NULL if any argument is NULL.
 * 
 * ```SQL
 * mysql> SELECT LEFT('foobarbar', 5);
 *        -> 'fooba'
 * ```
 */
export function left(string: Arg<string>, len: Arg<number>) {
  return new Col<string>({
    defer(q, context) {
      return `LEFT(${q.colRef(string, context)}, ${q.colRef(len, context)})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_length
 * 
 * Returns the length of the string str, measured in bytes. A multibyte character 
 * counts as multiple bytes. This means that for a string containing five 2-byte 
 * characters, LENGTH() returns 10, whereas CHAR_LENGTH() returns 5. Returns NULL 
 * if str is NULL.
 * 
 * ```SQL
 * mysql> SELECT LENGTH('text');
 *        -> 4
 * ```
 */
export function length(string: Arg<string>) {
  return new Col<number>({
    defer(q, context) {
      return `LENGTH(${q.colRef(string, context)})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_load-file
 * 
 * Reads the file and returns the file contents as a string. To use this function, 
 * the file must be located on the server host, you must specify the full path 
 * name to the file, and you must have the FILE privilege. The file must be readable 
 * by the server and its size less than max_allowed_packet bytes. If the 
 * secure_file_priv system variable is set to a nonempty directory name, the file to 
 * be loaded must be located in that directory. (Prior to MySQL 8.0.17, the file must 
 * be readable by all, not just readable by the server.)
 * 
 * If the file does not exist or cannot be read because one of the preceding conditions 
 * is not satisfied, the function returns NULL.
 * 
 * The character_set_filesystem system variable controls interpretation of file names 
 * that are given as literal strings.
 * 
 * See also: 
 * - [FILE](https://dev.mysql.com/doc/refman/8.0/en/privileges-provided.html#priv_file)
 * - [max_allowed_packet](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_max_allowed_packet)
 * - [character_set_filesystem](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_character_set_filesystem)
 * - [secure_file_priv](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_secure_file_priv)
 * 
 * ```SQL
 * mysql> UPDATE t
 *          SET blob_col=LOAD_FILE('/tmp/picture')
 *          WHERE id=1;
 * ```
 */
export function load_file(path: Arg<string>) {
  return new Col<string>({
    defer(q, context) {
      return `LOAD_FILE(${q.colRef(path, context)})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_locate
 * 
 * The first syntax returns the position of the first occurrence of substring substr 
 * in string str. The second syntax returns the position of the first occurrence of 
 * substring substr in string str, starting at position pos. Returns 0 if substr is 
 * not in str. Returns NULL if any argument is NULL.
 * 
 * ```SQL
 * mysql> SELECT LOCATE('bar', 'foobarbar');
 *        -> 4
 * mysql> SELECT LOCATE('xbar', 'foobar');
 *        -> 0
 * mysql> SELECT LOCATE('bar', 'foobarbar', 5);
 *        -> 7
 * ```
 */
export function locate(substr: Arg<string>, string: Arg<string>, pos?: Arg<number>) {
  return new Col<number>({
    defer(q, context) {
      return `LOCATE(${Array.prototype.reduce.call(arguments, (out, arg: Arg<string | number>) => {
        if (arg === undefined || arg === null)
          return out;

        const str = q.colRef(arg, context);
        return out && arg ? `${out}, ${str}` : str;
      }, '')})`
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_lpad
 * 
 * Returns the string str, left-padded with the string padstr to a length of len 
 * characters. If str is longer than len, the return value is shortened to len characters.
 * 
 * ```SQL
 * mysql> SELECT LPAD('hi',4,'??');
 *        -> '??hi'
 * mysql> SELECT LPAD('hi',1,'??');
 *        -> 'h'
 * ```
 */
export function lpad(str: Arg<string>, len: Arg<number>, padstr: Arg<string>) {
  return new Col<string>({
    defer(q, context) {
      return `LPAD(${q.colRef(str, context)}, ${q.colRef(len, context)}, ${q.colRef(padstr, context)})`;
    }
  });
}


/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_ltrim
 * 
 * Returns the string str with leading space characters removed. Returns NULL if str is NULL.
 * 
 * ```SQL
 * mysql> SELECT LTRIM('  barbar');
 *        -> 'barbar'
 * ```
 */
export function ltrim(str: Arg<string>) {
  return new Col<number>({
    defer(q, context) {
      return `LTRIM(${q.colRef(str, context)})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_make-set
 * 
 * Returns a set value (a string containing substrings separated by , characters) 
 * consisting of the strings that have the corresponding bit in bits set. str1 
 * corresponds to bit 0, str2 to bit 1, and so on. NULL values in str1, str2, ... 
 * are not appended to the result.
 * 
 * MySQL: 
 * ```SQL
 * mysql> SELECT MAKE_SET(1,'a','b','c');
 *        -> 'a'
 * mysql> SELECT MAKE_SET(1 | 4,'hello','nice','world');
 *        -> 'hello,world'
 * mysql> SELECT MAKE_SET(1 | 4,'hello','nice',NULL,'world');
 *        -> 'hello'
 * mysql> SELECT MAKE_SET(0,'a','b','c');
 *        -> ''
 * ```
 * 
 * TypeScript: 
 * ```typescript
 * make_set(1 | 4, 'hello', 'nice', 'world') // -> Col<string> -> 'hello,world'
 * ```
 */
export function make_set(bits: Arg<number>, ...args: Arg<string | null>[]) {
  return new Col<string>({
    defer(q, context) {
      return `MAKE_SET(${q.colRef(bits, context)}, ${args.reduce((out, arg) => {
        const str = q.colRef(arg, context);
        return out ? `${out}, ${str}` : str;
      }, '')}})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_oct
 * 
 * Returns a string representation of the octal value of N, where N is a longlong 
 * (BIGINT) number. This is equivalent to CONV(N,10,8). Returns NULL if N is NULL.
 * 
 * ```SQL
 * mysql> SELECT OCT(12);
 *        -> '14'
 * ```
 *  
 * See also: 
 * - [BIGINT](https://dev.mysql.com/doc/refman/8.0/en/integer-types.html)
 * - {@link conv}
 */
export function oct(big_int: Arg<number>) {
  return new Col<number>({
    defer(q, context) {
      return `OCT(${q.colRef(big_int, context)})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_ord
 * 
 * If the leftmost character of the string str is a multibyte character, returns the 
 * code for that character, calculated from the numeric values of its constituent 
 * bytes using this formula:
 * ```text
 *   (1st byte code)
 * + (2nd byte code * 256)
 * + (3rd byte code * 256^2) ...
 * ```
 * 
 * If the leftmost character is not a multibyte character, ORD() returns the same 
 * value as the ASCII() function. The function returns NULL if str is NULL.
 * 
 * ```SQL
 * mysql> SELECT ORD('2');
 *        -> 50
 * ```
 *  
 * See also: 
 * - // TODO: ASCII
 */
export function ord(str: Arg<string>) {
  return new Col<number>({
    defer(q, context) {
      return `ORD(${q.colRef(str, context)})`;
    }
  });
}


/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_position
 * 
 * POSITION(substr IN str) is a synonym for LOCATE(substr,str)
 * 
 * ```SQL
 * mysql> SELECT POSITION('wo' IN 'hello world');
 *        -> 7
 * ```
 * 
 * Usage:
 * ```typescript
 * position('wo', { in: 'hello world' }) // => Col<number> -> 7
 * ```
 *  
 * See also: 
 * - {@link locate}
 */
export function position(str: Arg<string>, arg: { in: Arg<string> }) {
  return new Col<number>({
    defer(q, context) {
      return `POSITION(${q.colRef(str, context)} IN ${q.colRef(arg.in, context)})`;
    }
  });
}


/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_quote
 * 
 * Quotes a string to produce a result that can be used as a properly escaped data 
 * value in an SQL statement. The string is returned enclosed by single quotation 
 * marks and with each instance of backslash (\), single quote ('), ASCII NUL, and 
 * Control+Z preceded by a backslash. If the argument is NULL, the return value is 
 * the word “NULL” without enclosing single quotation marks.
 * 
 * ```SQL
 * mysql> SELECT QUOTE('Don\'t!');
 *        -> 'Don\'t!'
 * mysql> SELECT QUOTE(NULL);
 *        -> NULL
 * ```
 * 
 * For comparison, see the quoting rules for literal strings and within the C API in 
 * [Section 9.1.1, “String Literals”](https://dev.mysql.com/doc/refman/8.0/en/string-literals.html), 
 * and [mysql_real_escape_string_quote()](https://dev.mysql.com/doc/c-api/8.0/en/mysql-real-escape-string-quote.html).
 *  
 */
export function quote(str: Arg<string>) {
  return new Col<string>({
    defer(q, context) {
      return `QUOTE(${q.colRef(str, context)})`;
    }
  });
}


/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_repeat
 * 
 * Returns a string consisting of the string str repeated count times. If count is less 
 * than 1, returns an empty string. Returns NULL if str or count is NULL.
 * 
 * ```SQL
 * mysql> SELECT REPEAT('MySQL', 3);
 *        -> 'MySQLMySQLMySQL'
 * ```
 */
export function repeat(str: Arg<string>, n: Arg<number>) {
  return new Col<string>({
    defer(q, context) {
      return `REPEAT(${q.colRef(str, context)}, ${q.colRef(n, context)})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_replace
 * 
 * Returns the string str with all occurrences of the string from_str replaced 
 * by the string to_str. REPLACE() performs a case-sensitive match when searching for from_str.
 * 
 * ```SQL
 * mysql> SELECT REPLACE('www.mysql.com', 'w', 'Ww');
 *        -> 'WwWwWw.mysql.com'
 * ```
 */
export function replace(str: Arg<string>, from: Arg<string>, to: Arg<string>) {
  return new Col<string>({
    defer(q, context) {
      return `REPLACE(${q.colRef(str, context)}, ${q.colRef(from, context)}, ${q.colRef(to, context)})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_reverse
 * 
 * Returns the string str with the order of the characters reversed, or NULL if str is NULL
 * 
 * ```SQL
 * mysql> SELECT REVERSE('abc');
 *        -> 'cba'
 * ```
 */
export function reverse(str: Arg<string>) {
  return new Col<string>({
    defer(q, context) {
      return `REVERSE(${q.colRef(str, context)})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_right
 * 
 * Returns the rightmost len characters from the string str, or NULL if any argument is NULL.
 * 
 * ```SQL
 * mysql> SELECT RIGHT('foobarbar', 4);
 *        -> 'rbar'
 * ```
 */
export function right(str: Arg<string>, len: Arg<number>) {
  return new Col<string>({
    defer(q, context) {
      return `RIGHT(${q.colRef(str, context)}, ${q.colRef(len, context)})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_rpad
 * 
 * Returns the string str, right-padded with the string padstr to a length of len characters. 
 * If str is longer than len, the return value is shortened to len characters. If str, 
 * padstr, or len is NULL, the function returns NULL.
 * 
 * ```SQL
 * mysql> SELECT RPAD('hi',5,'?');
 *        -> 'hi???'
 * mysql> SELECT RPAD('hi',1,'?');
 *        -> 'h'
 * ```
 */
export function rpad(str: Arg<string>, len: Arg<number>, padstr: Arg<string>) {
  return new Col<string>({
    defer(q, context) {
      return `RPAD(${q.colRef(str, context)}, ${q.colRef(len, context)}, ${q.colRef(padstr, context)})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_rtrim
 * 
 * Returns the string str with trailing space characters removed.
 * 
 * ```SQL
 * mysql> SELECT RTRIM('barbar   ');
 *        -> 'barbar'
 * ```
 */
export function rtrim(str: Arg<string>) {
  return new Col<string>({
    defer(q, context) {
      return `RTRIM(${q.colRef(str, context)})`;
    }
  });
}


/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#operator_sounds-like
 * 
 * This is the same as SOUNDEX(expr1) = SOUNDEX(expr2)
 * 
 * Usage:
 * ```typescript
 * soundex('time', { sounds_like: 'thyme' }) // => Col<boolean> -> true
 * ```
 */
export function soundex(str: Arg<string>, arg: { sounds_like: Arg<string> }): Col<boolean>

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_soundex
 * 
 * Returns a soundex string from str, or NULL if str is NULL. Two strings that sound 
 * almost the same should have identical soundex strings. A standard soundex string 
 * is four characters long, but the SOUNDEX() function returns an arbitrarily long string. 
 * You can use SUBSTRING() on the result to get a standard soundex string. All nonalphabetic 
 * characters in str are ignored. All international alphabetic characters outside the A-Z 
 * range are treated as vowels.
 * 
 * **IMPORTANT: When using SOUNDEX(), you should be aware of the following limitations:**
 * - This function, as currently implemented, is intended to work well with strings that are in the 
 * English language only. Strings in other languages may not produce reliable results.
 * - This function is not guaranteed to provide consistent results with strings that use multibyte 
 * character sets, including utf-8. See Bug #22638 for more information.
 * 
 * ```SQL
 * mysql> SELECT SOUNDEX('Hello');
 *        -> 'H400'
 * mysql> SELECT SOUNDEX('Quadratically');
 *        -> 'Q36324'
 * ```
 * 
 * **Note:**
 * This function implements the original Soundex algorithm, not the more popular enhanced version 
 * (also described by D. Knuth). The difference is that original version discards vowels first 
 * and duplicates second, whereas the enhanced version discards duplicates first and vowels second.
 */
export function soundex(str: Arg<string>): Col<string>;
export function soundex(str: Arg<string>, arg?: { sounds_like: Arg<string> }) {

  if (arg)
    return new Col<boolean>({
      defer(q, context) {
        return `${q.colRef(str, context)} SOUNDS LIKE ${q.colRef(arg.sounds_like, context)}`;
      }
    });

  else
    return new Col<string>({
      defer(q, context) {
        return `SOUNDEX(${q.colRef(str, context)})`;
      }
    });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_space
 * 
 * Returns a string consisting of N space characters, or NULL if N is NULL.
 * 
 * ```SQL
 * mysql> SELECT SPACE(6);
 *        -> '      '
 * ```
 */
export function space(n: Arg<number>) {
  return new Col<string>({
    defer(q, context) {
      return `SPACE(${q.colRef(n, context)})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_substr
 * 
 * The forms without a len argument return a substring from string str starting at position pos. 
 * 
 * The forms with a len argument return a substring len characters long from string str, 
 * starting at position pos. The forms that use FROM are standard SQL syntax. It is also 
 * possible to use a negative value for pos. In this case, the beginning of the substring is pos 
 * characters from the end of the string, rather than the beginning. A negative value may be used 
 * for pos in any of the forms of this function. A value of 0 for pos returns an empty string. 
 * 
 * For all forms of SUBSTRING(), the position of the first character in the string from which the 
 * substring is to be extracted is reckoned as 1.
 * 
 * SQL:
 * ```SQL
 * mysql> SELECT SUBSTRING('foobarbar' FROM 4);
 *        -> 'barbar'
 * mysql> SELECT SUBSTRING('Sakila' FROM -4 FOR 2);
 *        -> 'ki'
 * ```
 * 
 * Usage: 
 * ```typescript
 * substring('foobarbar', { from: 4 }) // => Col<string> -> SUBSTRING('foobarbar', 4)
 * substring('Sakila', { from: -4, for: 2 }) // => Col<string> -> SUBSTRING('Sakila', -4, 2)
 * ```
 */
export function substring(str: Arg<string>, opts: { from: Arg<number>, for?: Arg<number> }): Col<string>;

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_substr
 * 
 * The forms without a len argument return a substring from string str starting at position pos. 
 * 
 * The forms with a len argument return a substring len characters long from string str, 
 * starting at position pos. The forms that use FROM are standard SQL syntax. It is also 
 * possible to use a negative value for pos. In this case, the beginning of the substring is pos 
 * characters from the end of the string, rather than the beginning. A negative value may be used 
 * for pos in any of the forms of this function. A value of 0 for pos returns an empty string. 
 * 
 * For all forms of SUBSTRING(), the position of the first character in the string from which the 
 * substring is to be extracted is reckoned as 1.
 * 
 * SQL:
 * ```SQL
 * mysql> SELECT SUBSTRING('foobarbar', 4);
 *        -> 'barbar'
 * mysql> SELECT SUBSTRING('Sakila', -4, 2);
 *        -> 'ki'
 * ```
 * 
 * Usage: 
 * ```typescript
 * substring('foobarbar', 4) // => Col<string> -> SUBSTRING('foobarbar', 4)
 * substring('Sakila', -4, 2) // => Col<string> -> SUBSTRING('Sakila', -4, 2)
 * ```
 */
export function substring(str: Arg<string>, pos: Arg<number>, len?: Arg<number>): Col<string>;
export function substring(arg1: Arg<string>, arg2: Arg<number> | { from: Arg<number>, for?: Arg<number> }, arg3?: Arg<number>): Col<string> {

  const target = arg1;
  let pos: Arg<number>;
  let len: Arg<number> | undefined = undefined;

  if (typeof arg2 === 'number' || arg2 instanceof Col)
    pos = arg2;
  else {
    pos = arg2.from;
    len = arg2.for;
  }

  if (arg3)
    pos = arg3;

  return new Col<string>({
    defer(q, context) {
      return `SUBSTRING(${[target, pos, len].reduce((out, arg) => {
        if (!arg)
          return out;

        const str = q.colRef(arg, context);
        return out ? `${out}, ${str}` : str;

      }, '')})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_substring-index
 * 
 * Returns the substring from string `str` before count occurrences of the delimiter `delim`. 
 * If `count` is positive, everything to the left of the final delimiter (counting from the left) 
 * is returned. If `count` is negative, everything to the right of the final delimiter (counting 
 * from the right) is returned. `SUBSTRING_INDEX()` performs a case-sensitive match when searching 
 * for delim.
 * 
 * This function is multibyte safe. 
 * 
 * `SUBSTRING_INDEX()` returns `NULL` if any of its arguments are `NULL`.
 * 
 * ```SQL
 * mysql> SELECT SUBSTRING_INDEX('www.mysql.com', '.', 2);
 *        -> 'www.mysql'
 * mysql> SELECT SUBSTRING_INDEX('www.mysql.com', '.', -2);
 *        -> 'mysql.com'
 * ```
 * 
 */
export function substring_index(str: Arg<string>, delim: Arg<string>, count: Arg<number>): Col<string> {
  return new Col<string>({
    defer(q, context) {
      return `SUBSTRING_INDEX(${q.colRef(str, context)}, ${q.colRef(delim, context)}, ${q.colRef(count, context)})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_to-base64
 * 
 * Converts the string argument to base-64 encoded form and returns the result as a character 
 * string with the connection character set and collation. If the argument is not a string, 
 * it is converted to a string before conversion takes place. The result is `NULL` if the argument 
 * is `NULL`. Base-64 encoded strings can be decoded using the `FROM_BASE64()` function.
 * 
 * ```SQL
 * mysql> SELECT TO_BASE64('abc'), FROM_BASE64(TO_BASE64('abc'));
 *        -> 'JWJj', 'abc'
 * ```
 * 
 * Different base-64 encoding schemes exist. These are the encoding and decoding rules used by 
 * `TO_BASE64()` and `FROM_BASE64()`:
 * - The encoding for alphabet value `62` is `'+'`.
 * - The encoding for alphabet value `63` is `'/'`.
 * - Encoded output consists of groups of 4 printable characters. Each 3 bytes of the input data 
 * are encoded using 4 characters. If the last group is incomplete, it is padded with `'='` characters 
 * to a length of 4.
 * - A newline is added after each 76 characters of encoded output to divide long output into multiple lines.
 * - Decoding recognizes and ignores newline, carriage return, tab, and space.
 * 
 * See also: 
 * - {@link from_base64}
 */
export function to_base64(str: Arg<string>): Col<string> {
  return new Col<string>({
    defer(q, context) {
      return `TO_BASE64(${q.colRef(str, context)})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_trim
 * 
 * Returns the string str with all remstr prefixes or suffixes removed. If none of the 
 * specifiers `BOTH`, `LEADING`, or `TRAILING` is given, `BOTH` is assumed. 
 * 
 * `remstr` is optional and, if not specified, spaces are removed.
 * 
 * ```SQL
 * mysql> SELECT TRIM(TRAILING 'xyz' FROM 'barxxyz');
 *        -> 'barx'
 * ```
 * 
 * Usage:
 * ```typescript
 * trim({ trailing: 'xyzz', from: 'barxxyz'}) // => Col<string> -> TRIM(TRAILING 'xyz' FROM 'barxxyz')
 * ```
 * 
 * This function is multibyte safe. It returns `NULL` if any of its arguments are `NULL`.
 */
export function trim(args: { trailing: Arg<string>, from: Arg<string> }): Col<string>;

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_trim
 * 
 * Returns the string str with all remstr prefixes or suffixes removed. If none of the 
 * specifiers `BOTH`, `LEADING`, or `TRAILING` is given, `BOTH` is assumed. 
 * 
 * `remstr` is optional and, if not specified, spaces are removed.
 * 
 * ```SQL
 * mysql> SELECT TRIM(LEADING 'x' FROM 'xxxbarxxx');
 *        -> 'barxxx'
 * ```
 * 
 * Usage:
 * ```typescript
 * trim({ leading: 'x', from: 'xxxbarxxx'}) // => Col<string> -> TRIM(LEADING 'x' FROM 'xxxbarxxx')
 * ```
 * 
 * This function is multibyte safe. It returns `NULL` if any of its arguments are `NULL`.
 */
export function trim(args: { leading: Arg<string>, from: Arg<string> }): Col<string>;

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_trim
 * 
 * Returns the string str with all remstr prefixes or suffixes removed. If none of the 
 * specifiers `BOTH`, `LEADING`, or `TRAILING` is given, `BOTH` is assumed. 
 * 
 * `remstr` is optional and, if not specified, spaces are removed.
 * 
 * ```SQL
 * mysql> SELECT TRIM(BOTH 'x' FROM 'xxxbarxxx');
 *        -> 'bar'
 * ```
 * 
 * Usage:
 * ```typescript
 * trim({ both: 'x', from: 'xxxbarxxx'}) // => Col<string> -> TRIM(BOTH 'x' FROM 'xxxbarxxx')
 * ```
 * 
 * This function is multibyte safe. It returns `NULL` if any of its arguments are `NULL`.
 */
export function trim(args: { both: Arg<string>, from: Arg<string> }): Col<string>;

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_trim
 * 
 * Returns the string str with all remstr prefixes or suffixes removed. If none of the 
 * specifiers `BOTH`, `LEADING`, or `TRAILING` is given, `BOTH` is assumed. 
 * 
 * `remstr` is optional and, if not specified, spaces are removed.
 * 
 * ```SQL
 * mysql> SELECT TRIM('  bar   ');
 *        -> 'bar'
 * ```
 * 
 * This function is multibyte safe. It returns `NULL` if any of its arguments are `NULL`.
 */
export function trim(str: Arg<string>): Col<string>;
export function trim(arg: any): Col<string> {

  if (typeof arg === 'string' || arg instanceof Col)
    return new Col<string>({
      defer(q, context) {
        return `TRIM(${q.colRef(arg, context)})`;
      }
    });

  const from: Arg<string> = arg.from;
  const [specifier, value]: [string, Arg<string>] = (() => {

    for (const key in arg)
      switch (key) {
        case 'both':
          return ['BOTH', arg[key]];
        case 'leading':
          return ['LEADING', arg[key]];
        case 'trailing':
          return ['TRAILING', arg[key]];
      }

    throw new Error('Expected both, leading, or trailing specifier in trim() method');

  })();

  return new Col<string>({
    defer(q, context) {
      return `TRIM(${specifier} ${q.colRef(value, context)} FROM ${q.colRef(from, context)})`;
    }
  });
}

/**
 * https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_ucase
 * 
 * `UCASE()` is a synonym for `UPPER()`.
 * 
 * `UCASE()` used within views is rewritten as `UPPER()`.
 * 
 * See also: 
 * - {@link upper}
 */
export const ucase = upper;

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_unhex
 * 
 * For a string argument `str`, `UNHEX(str)` interprets each pair of characters in the argument 
 * as a hexadecimal number and converts it to the byte represented by the number. 
 * The return value is a binary string.
 * 
 * ```SQL
 * mysql> SELECT UNHEX('4D7953514C');
 *        -> 'MySQL'
 * mysql> SELECT X'4D7953514C';
 *        -> 'MySQL'
 * mysql> SELECT UNHEX(HEX('string'));
 *        -> 'string'
 * mysql> SELECT HEX(UNHEX('1267'));
 *        -> '1267'
 * ```
 * 
 * The characters in the argument string must be legal hexadecimal digits: `'0' .. '9', 'A' .. 'F', 'a' .. 'f'`. 
 * If the argument contains any nonhexadecimal digits, or is itself `NULL`, the result is `NULL`:
 * 
 * ```SQL
 * mysql> SELECT UNHEX('GG');
 * ```
 * ```text
 * +-------------+
 * | UNHEX('GG') |
 * +-------------+
 * | NULL        |
 * +-------------+
 * ```
 * ```SQL
 * mysql> SELECT UNHEX(NULL);
 * ```
 * ```text
 * +-------------+
 * | UNHEX(NULL) |
 * +-------------+
 * | NULL        |
 * +-------------+
 * ```
 * 
 * A `NULL` result can also occur if the argument to `UNHEX()` is a `BINARY` column, because values are 
 * padded with `0x00` bytes when stored but those bytes are not stripped on retrieval. For example, 
 * `'41'` is stored into a `CHAR(3)` column as `'41 '` and retrieved as `'41'` (with the trailing pad space 
 * stripped), so `UNHEX()` for the column value returns `X'41'`. By contrast, `'41'` is stored into a 
 * `BINARY(3)` column as `'41\0'` and retrieved as `'41\0'` (with the trailing pad `0x00` byte not stripped). 
 * `'\0'` is not a legal hexadecimal digit, so `UNHEX()` for the column value returns `NULL`. 
 * 
 * For a numeric argument `N`, the inverse of `HEX(N)` is not performed by `UNHEX()`. Use `CONV(HEX(N),16,10)` instead.
 *  
 * See the description of `HEX()`. If `UNHEX()` is invoked from within the mysql client, binary strings 
 * display using hexadecimal notation, depending on the value of the 
 * [`--binary-as-hex`](https://dev.mysql.com/doc/refman/8.0/en/mysql-command-options.html#option_mysql_binary-as-hex). For more 
 * information about that option, see 
 * [Section 4.5.1, “mysql — The MySQL Command-Line Client”](https://dev.mysql.com/doc/refman/8.0/en/mysql.html).
 * 
 * See also: 
 * - {@link hex}
 * - {@link conv}
 */
export function unhex(str: Arg<string>) {
  return new Col<string>({
    defer(q, context) {
      return `UPPER(${q.colRef(str, context)})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_upper
 * 
 * Returns the string str with all characters changed to uppercase according to the current character 
 * set mapping, or `NULL` if str is `NULL`. The default character set is `utf8mb4`.
 * 
 * ```SQL
 * mysql> SELECT UPPER('Hej');
 *        -> 'HEJ'
 * ```
 * 
 * See the description of `LOWER()` for information that also applies to `UPPER()`. This included 
 * information about how to perform lettercase conversion of binary strings (`BINARY`, `VARBINARY`, `BLOB`) 
 * for which these functions are ineffective, and information about case folding for Unicode character sets.
 *
 * This function is multibyte safe.
 * 
 * See also:
 * - {@link lower}
 * 
 */
export function upper(str: Arg<string>) {
  return new Col<string>({
    defer(q, context) {
      return `UPPER(${q.colRef(str, context)})`;
    }
  });
}

// #endregion

//#region Date Operations

export function timestamp(value: Arg<string> | Date) {
  return operation<string>((q, ctx) => {

    if (value instanceof Date)
      return `TIMESTAMP(${q.paramaterize(value.toISOString())})`;

    else
      return `TIMESTAMP(${q.colRef(value, ctx)})`;

  });
}

//#endregion

// #region COMPARISON FUNCTIONS

/**
 * Refs: 
 * - https://dev.mysql.com/doc/refman/8.0/en/comparison-operators.html#operator_is
 * - https://dev.mysql.com/doc/refman/8.0/en/comparison-operators.html#operator_is-null
 * 
 * Tests a value against a boolean value, where `boolean_value` can be `TRUE`, `FALSE`, or `UNKNOWN`.
 * 
 * ```SQL
 * mysql> SELECT 1 IS TRUE, 0 IS FALSE, NULL IS UNKNOWN;
 *         -> 1, 1, 1
 * ```
 * 
 * ### Usage
 * `NULL` and `UNKNOWN` are defined as constants within the mysql-query-builder library. 
 * JavasScript primitive `null` will be translated to `NULL` but may have unintended outcomes if used
 * as a return type from another function evaluated at run time. 
 * 
 * It is prefferred to explicitly pass `NULL` and `UNKNOWN`.
 * 
 * ```typescript
 * import { is, NULL, UNKNOWN } from 'mysql-query-builder';
 * 
 * is(1, NULL); // => Col<boolean> -> 1 IS NULL
 * is(1, null); // => Col<boolean> -> 1 IS NULL
 * is(1, UNKNOWN); // => Col<boolean> -> 1 IS UNKNOWN
 * is(1, true); // => Col<boolean> -> 1 IS TRUE
 * ```
 */
export function is(target: Arg<booleanish>, value: Arg<boolean> | typeof NULL | typeof UNKNOWN | null) {
  return new Col<boolean>({
    defer(q, ctx) {
      if (NULL.equals(value))
        return `${q.colRef(target, ctx)} IS NULL`;

      if (UNKNOWN.equals(value))
        return `${q.colRef(target, ctx)} IS UNKNOWN`;

      return `${q.colRef(target, ctx)} IS ${q.colRef(value, ctx)}`;
    },
  });
}

/**
 * Refs: 
 * - https://dev.mysql.com/doc/refman/8.0/en/comparison-operators.html#operator_is-not
 * - https://dev.mysql.com/doc/refman/8.0/en/comparison-operators.html#operator_is-not-null
 * 
 * Tests a value against a boolean value, where boolean_value can be `TRUE`, `FALSE`, or `UNKNOWN`.
 * 
 * ```SQL
 * mysql> SELECT 1 IS NOT UNKNOWN, 0 IS NOT UNKNOWN, NULL IS NOT UNKNOWN;
 *         -> 1, 1, 0
 * ```
  * ### Usage
 * `NULL` and `UNKNOWN` are defined as constants within the mysql-query-builder library. 
 * JavasScript primitive `null` will be translated to `NULL` but may have unintended outcomes if used
 * as a return type from another function evaluated at run time. 
 * 
 * It is prefferred to explicitly pass `NULL` and `UNKNOWN`.
 * 
 * ```typescript
 * import { is_not, NULL, UNKNOWN } from 'mysql-query-builder';
 * 
 * is_not(1, NULL); // => Col<boolean> -> 1 IS NOT NULL
 * is_not(1, null); // => Col<boolean> -> 1 IS NOT NULL
 * is_not(1, UNKNOWN); // => Col<boolean> -> 1 IS NOT UNKNOWN
 * is_not(1, true); // => Col<boolean> -> 1 IS NOT TRUE
 * ```
 */
export function is_not(target: Arg, value: Arg | null) {
  return new Col<boolean>({
    defer(q, ctx) {
      if (NULL.equals(value))
        return `${q.colRef(target, ctx)} IS NOT NULL`;

      if (UNKNOWN.equals(value))
        return `${q.colRef(target, ctx)} IS NOT UNKNOWN`;

      return `${q.colRef(target, ctx)} IS ${q.colRef(value, ctx)}`;
    },
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/comparison-operators.html#function_isnull
 * 
 * If expr is `NULL`, `ISNULL()` returns `1`, otherwise it returns `0`.
 * 
 * ```SQL
 * mysql> SELECT ISNULL(1+1);
 *         -> 0
 * mysql> SELECT ISNULL(1/0);
 *         -> 1
 * ```
 * 
 * `ISNULL()` can be used instead of = to test whether a value is `NULL`. (Comparing a value to 
 * `NULL` using = always yields `NULL`.)
 * 
 * The `ISNULL()` function shares some special behaviors with the IS `NULL` comparison operator. 
 * See the description of IS `NULL`.
 * 
 * See also: 
 * - {@link is}
 * - {@link equalTo}
 */
export function isnull(target: Arg): Col<boolean> {
  return new Col({
    defer(q, context) {
      return `ISNULL(${q.colRef(target, context)})`;
    },
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/flow-control-functions.html#function_ifnull
 * 
 * If `expr1` is not `NULL`, `IFNULL()` returns `expr1`; otherwise it returns `expr2`.
 * 
 * ```SQL
 * mysql> SELECT IFNULL(1, 0);
 *         -> 1
 * mysql> SELECT IFNULL(NULL, 10);
 *         -> 10
 * mysql> SELECT IFNULL(1 / 0, 10);
 *         -> 10
 * mysql> SELECT IFNULL(1 / 0, 'yes');
 *         -> 'yes'
 * ```
 * 
 * The default return type of `IFNULL(expr1,expr2)` is the more “general” of the two expressions, 
 * in the order `STRING`, `REAL`, or `INTEGER`. Consider the case of a table based on expressions 
 * or where MySQL must internally store a value returned by `IFNULL()` in a temporary table:
 * 
 * ```SQL
 * mysql> CREATE TABLE tmp SELECT IFNULL(1,'test') AS test;
 * mysql> DESCRIBE tmp;
 * ```
 * ```text
 * +-------+--------------+------+-----+---------+-------+
 * | Field | Type         | Null | Key | Default | Extra |
 * +-------+--------------+------+-----+---------+-------+
 * | test  | varbinary(4) | NO   |     |         |       |
 * +-------+--------------+------+-----+---------+-------+
 * ```
 * 
 * In this example, the type of the test column is `VARBINARY(4)` (a string type).
 * 
 */
export function ifnull<T, U>(expr1: Arg<T>, expr2: Arg<U>): Col<T | U> {
  return new Col({
    defer(q, context) {
      return `IFNULL(${q.colRef(expr1, context)}, ${q.colRef(expr2, context)})`;
    },
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/logical-operators.html#operator_and
 * 
 * Logical `AND`. Evaluates to `1` if all operands are nonzero and not `NULL`, to `0` if one or more 
 * operands are `0`, otherwise `NULL` is returned.
 * 
 * ```SQL
 * mysql> SELECT 1 AND 1;
 *         -> 1
 * mysql> SELECT 1 AND 0;
 *         -> 0
 * mysql> SELECT 1 AND NULL;
 *         -> NULL
 * mysql> SELECT 0 AND NULL;
 *         -> 0
 * mysql> SELECT NULL AND 0;
 *         -> 0
 * ```
 * 
 * The `&&` operator is a nonstandard MySQL extension. As of MySQL 8.0.17, this operator is deprecated; 
 * expect support for it to be removed in a future version of MySQL. Applications should be adjusted 
 * to use the standard SQL `AND` operator.
 */
export function and(...args: Col<boolean>[]): Col<boolean> {
  return new Col({
    defer(q, context) {
      return `(${args.reduce((out, arg) => {
        const str = q.colRef(arg, context);
        return out ? `${out} AND ${str}` : str;
      }, '')})`
    },
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/logical-operators.html#operator_or
 * 
 * Logical `OR`. When both operands are non-`NULL`, the result is `1` if any operand is nonzero, 
 * and `0` otherwise. With a `NULL` operand, the result is `1` if the other operand is nonzero, 
 * and `NULL` otherwise. If both operands are `NULL`, the result is `NULL`.
 * 
 * ```SQL
 * mysql> SELECT 1 OR 1;
 *         -> 1
 * mysql> SELECT 1 OR 0;
 *         -> 1
 * mysql> SELECT 0 OR 0;
 *         -> 0
 * mysql> SELECT 0 OR NULL;
 *         -> NULL
 * mysql> SELECT 1 OR NULL;
 *         -> 1
 * ```
 * 
 * ### Note
 * > If the 
 * > [`PIPES_AS_CONCAT`](https://dev.mysql.com/doc/refman/8.0/en/sql-mode.html#sqlmode_pipes_as_concat) 
 * > SQL mode is enabled, `||` signifies the SQL-standard string concatenation operator (like `CONCAT()`).
 * 
 * The `||` operator is a nonstandard MySQL extension. As of MySQL 8.0.17, this operator is deprecated; 
 * expect support for it to be removed in a future version of MySQL. Applications should be adjusted 
 * to use the standard SQL `OR` operator. Exception: Deprecation does not apply if `PIPES_AS_CONCAT` 
 * is enabled because, in that case, `||` signifies string concatenation.
 */
export function or(...args: Col<boolean>[]): Col<boolean> {
  return new Col({
    defer(q, context) {
      return `(${args.reduce((out, arg) => {
        const str = q.colRef(arg, context);
        return out ? `${out} OR ${str}` : str;
      }, '')})`
    },
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/logical-operators.html#operator_xor
 * 
 * Logical `XOR`. Returns `NULL` if either operand is `NULL`. For non-`NULL` operands, evaluates 
 * to `1` if an odd number of operands is nonzero, otherwise `0` is returned.
 * 
 * ```SQL
 * mysql> SELECT 1 XOR 1;
 *         -> 0
 * mysql> SELECT 1 XOR 0;
 *         -> 1
 * mysql> SELECT 1 XOR NULL;
 *         -> NULL
 * mysql> SELECT 1 XOR 1 XOR 1;
 *         -> 1
 * ```
 * 
 * `a XOR b` is mathematically equal to `(a AND (NOT b)) OR ((NOT a) and b)`.
 */
export function xor(...args: Col<boolean>[]) {
  return new Col<boolean>({
    defer(q, context) {
      return args.reduce((out, col) => {
        const ref = q.colRef(col, context);
        return out ? ` XOR ${ref}` : ref;
      }, '')
    },
  })
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/comparison-operators.html#operator_between
 * 
 * If `expr` is greater than or equal to min and `expr` is less than or equal to max, `BETWEEN` returns `1`, 
 * otherwise it returns `0`. This is equivalent to the expression `(min <= expr AND expr <= max)` if all 
 * the arguments are of the same type. Otherwise type conversion takes place according to the rules described in 
 * [Section 12.3, “Type Conversion in Expression Evaluation”](https://dev.mysql.com/doc/refman/8.0/en/type-conversion.html), 
 * but applied to all the three arguments.
 * 
 * ```SQL
 * mysql> SELECT 2 BETWEEN 1 AND 3, 2 BETWEEN 3 and 1;
 *         -> 1, 0
 * mysql> SELECT 1 BETWEEN 2 AND 3;
 *         -> 0
 * mysql> SELECT 'b' BETWEEN 'a' AND 'c';
 *         -> 1
 * mysql> SELECT 2 BETWEEN 2 AND '3';
 *         -> 1
 * mysql> SELECT 2 BETWEEN 2 AND 'x-3';
 *         -> 0
 * ```
 * 
 * For best results when using `BETWEEN` with date or time values, use `CAST()` to explicitly convert the values 
 * to the desired data type. Examples: If you compare a `DATETIME` to two `DATE` values, convert the `DATE` values to 
 * `DATETIME` values. If you use a string constant such as `'2001-1-1'` in a comparison to a `DATE`, cast the 
 * string to a `DATE`.
 */
export function between<T>(target: Arg<T>, start: Arg<T>, end: Arg<T>) {
  return new Col<boolean>({
    defer(q, context) {
      return `${q.colRef(target, context)} BETWEEN ${q.colRef(start, context)} AND ${q.colRef(end, context)}`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/comparison-operators.html#operator_not-between
 * 
 * This is the same as `NOT (expr BETWEEN min AND max)`.
 * 
 * See also: 
 * - {@link between}
 */
export function not_between<T>(target: Arg<T>, start: Arg<T>, end: Arg<T>) {
  return new Col<boolean>({
    defer(q, context) {
      return `${q.colRef(target, context)} NOT BETWEEN ${q.colRef(start, context)} AND ${q.colRef(end, context)}`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/comparison-operators.html#function_coalesce
 * 
 * Returns the first non-`NULL` value in the list, or `NULL` if there are no non-`NULL` values.
 * 
 * The return type of `COALESCE()` is the aggregated type of the argument types.
 * 
 * ```SQL
 * mysql> SELECT COALESCE(NULL,1);
 *         -> 1
 * mysql> SELECT COALESCE(NULL,NULL,NULL);
 *         -> NULL
 * ```
 */
export function coalesce(...args: Arg<any>[]) {
  return new Col<any>({
    defer(q, context) {
      return `COALESCE(${args.reduce((out, arg) => {
        const ref = q.colRef(arg, context);
        return out ? `${out}, ${ref}` : ref;
      }, '')})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/comparison-operators.html#function_greatest
 * 
 * With two or more arguments, returns the largest (maximum-valued) argument. The arguments 
 * are compared using the same rules as for `LEAST()`.
 * 
 * ```SQL
 * mysql> SELECT GREATEST(2,0);
 *         -> 2
 * mysql> SELECT GREATEST(34.0,3.0,5.0,767.0);
 *         -> 767.0
 * mysql> SELECT GREATEST('B','A','C');
 *         -> 'C'
 * ```
 * 
 * See also: 
 * - {@link least}
 */
export function greatest<T extends string | number>(...args: Arg<T>[]) {
  return new Col<T>({
    defer(q, context) {
      return `GREATEST(${args.reduce((out, arg) => {
        const ref = q.colRef(arg, context);
        return out ? `${out}, ${ref}` : ref;
      }, '')})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/comparison-operators.html#function_least
 * 
 * With two or more arguments, returns the smallest (minimum-valued) argument. The arguments are 
 * compared using the following rules:
 * - If any argument is `NULL`, the result is `NULL`. No comparison is needed.
 * - If all arguments are integer-valued, they are compared as integers.
 * - If at least one argument is double precision, they are compared as double-precision values. 
 * Otherwise, if at least one argument is a `DECIMAL` value, they are compared as `DECIMAL` values.
 * - If the arguments comprise a mix of numbers and strings, they are compared as strings.
 * - If any argument is a nonbinary (character) string, the arguments are compared as nonbinary strings.
 * - In all other cases, the arguments are compared as binary strings.
 * 
 * The return type of `LEAST()` is the aggregated type of the comparison argument types
 * 
 * ```SQL
 * mysql> SELECT LEAST(2, 0);
 *         -> 0
 * mysql> SELECT LEAST(34.0, 3.0, 5.0, 767.0);
 *         -> 3.0
 * mysql> SELECT LEAST('B', 'A', 'C');
 *         -> 'A'
 * ```
 * 
 * See also: 
 * - {@link greatest}
 */
export function least<T extends string | number>(...args: Arg<T>[]) {
  return new Col<T>({
    defer(q, context) {
      return `LEAST(${args.reduce((out, arg) => {
        const ref = q.colRef(arg, context);
        return out ? `${out}, ${ref}` : ref;
      }, '')})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/comparison-operators.html#operator_in
 * 
 * Returns `1` (true) if `expr` is equal to any of the values in the `IN()` list, else returns `0` (false).
 * 
 * ```SQL
 * mysql> SELECT 2 IN (0,3,5,7);
 *         -> 0
 * mysql> SELECT 'wefwf' IN ('wee','wefwf','weg');
 *         -> 1
 * ```
 * 
 * Type conversion takes place according to the rules described in 
 * [Section 12.3, “Type Conversion in Expression Evaluation”](https://dev.mysql.com/doc/refman/8.0/en/type-conversion.html),
 * applied to all the arguments. If no type conversion is needed for the values in the `IN()` list, 
 * they are all non-JSON constants of the same type, and `expr` can be compared to each of them as 
 * a value of the same type (possibly after type conversion), an optimization takes place. 
 * The values the list are sorted and the search for `expr` is done using a binary search, which 
 * makes the `IN()` operation very quick.
 * 
 * IN() can be used to compare row constructors:
 * 
 * ```SQL
 * mysql> SELECT (3,4) IN ((1,2), (3,4));
 *         -> 1
 * mysql> SELECT (3,4) IN ((1,2), (3,5));
 *         -> 0
 * ```
 * 
 * You should never mix quoted and unquoted values in an IN() list because the comparison rules for quoted 
 * values (such as strings) and unquoted values (such as numbers) differ. Mixing types may therefore 
 * lead to inconsistent results. 
 * 
 * **For example**
 * ```SQL
 * SELECT val1 FROM tbl1 WHERE val1 IN (1, 2, 'a'); -- BAD
 * SELECT val1 FROM tbl1 WHERE val1 IN ('1', '2', 'a'); -- GOOD
 * ```
 * 
 * The number of values in the `IN()` list is only limited by the 
 * [max_allowed_packet](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_max_allowed_packet) 
 * value.
 * 
 * To comply with the SQL standard, `IN()` returns `NULL` not only if the expression on the left hand side is `NULL`, 
 * but also if no match is found in the list and one of the expressions in the list is `NULL`.
 * 
 * `IN()` syntax can also be used to write certain types of subqueries. See 
 * [Section 13.2.15.3, “Subqueries with ANY, IN, or SOME”](https://dev.mysql.com/doc/refman/8.0/en/any-in-some-subqueries.html).
 * 
 * See also: 
 * - {@link not_in}
 */
export function iin<T>(target: Arg<T>, values: Arg<T>[]) {
  return new Col<T>({
    defer(q, context) {
      return `${q.colRef(target, context)} IN (${values.reduce((out, arg) => {
        const ref = q.colRef(arg, context);
        return out ? `${out}, ${ref}` : ref;
      }, '')})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/comparison-operators.html#operator_not-in
 * 
 * This is the same as `NOT (expr IN (value,...))`.
 * 
 * ```SQL
 * mysql> SELECT 2 NOT IN (0,3,5,7);
 *         -> 1
 * mysql> SELECT 'wefwf' NOT IN ('wee','wefwf','weg');
 *         -> 0
 * ```
 * 
 * See also: 
 * - {@link iin}
 */
export function not_in<T>(target: Arg<T>, values: Arg<T>[]) {
  return new Col<T>({
    defer(q, context) {
      return `${q.colRef(target, context)} NOT IN (${values.reduce((out, arg) => {
        const ref = q.colRef(arg, context);
        return out ? `${out}, ${ref}` : ref;
      }, '')})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/comparison-operators.html#function_interval
 * 
 * Returns `0` if `N < N1`, `1` if `N < N2` and so on or `-1` if `N` is `NULL`. All arguments are 
 * treated as integers. It is required that `N1 < N2 < N3 < ... < Nn` for this function to work correctly. 
 * This is because a binary search is used (very fast).
 * 
 * ```SQL
 * mysql> SELECT INTERVAL(23, 1, 15, 17, 30, 44, 200);
 *         -> 3
 * mysql> SELECT INTERVAL(10, 1, 10, 100, 1000);
 *         -> 2
 * mysql> SELECT INTERVAL(22, 23, 30, 44, 200);
 *         -> 0
 * ```
 */
export function interval(...values: Arg<number>[]) {
  return new Col<number>({
    defer(q, context) {
      return `INTERVAL(${values.reduce((out, arg) => {
        const ref = q.colRef(arg, context);
        return out ? `${out}, ${ref}` : ref;
      }, '')})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-comparison-functions.html#operator_like
 * 
 * Pattern matching using an SQL pattern. Returns `1` (`TRUE`) or `0` (`FALSE`). If either `expr` 
 * or `pat` is `NULL`, the result is `NULL`.
 * 
 * With LIKE you can use the following two wildcard characters in the pattern:
 * - `%` matches any number of characters, even zero characters.
 * - `_` matches exactly one character.
 * 
 * ```SQL
 * mysql> SELECT 'David!' LIKE 'David_';
 *         -> 1
 * mysql> SELECT 'David!' LIKE '%D%v%';
 *         -> 1
 * ```
 * 
 * As an extension to standard SQL, MySQL permits LIKE on numeric expressions.
 * ```SQL
 * mysql> SELECT 10 LIKE '1%';
 *         -> 1
 * ```
 * 
 * ### Escaping and Wildcard Literals
 * 
 * To test for literal instances of a wildcard character, precede it by the escape character. 
 * If you do not specify the `ESCAPE` character, `\` is assumed, unless the 
 * [`NO_BACKSLASH_ESCAPES`](https://dev.mysql.com/doc/refman/8.0/en/sql-mode.html#sqlmode_no_backslash_escapes) 
 * SQL mode is enabled. In that case, no escape character is used.
 * - `\%` matches one `%` character.
 * - `\_` matches one `_`character.
 * 
 * ```SQL
 * mysql> SELECT 'David!' LIKE 'David\_';
 *         -> 0
 * mysql> SELECT 'David_' LIKE 'David\_';
 *         -> 1
 * ```
 * 
 * To specify a different escape character, use the ESCAPE clause:
 * ```SQL
 * mysql> SELECT 'David_' LIKE 'David|_' ESCAPE '|';
 *         -> 1
 * ```
 * 
 * ```typescript
 * // TypeScript
 * like('David_', 'David|_', { escape: '|' }) // => Col<boolean> -> 'David_' LIKE 'David|_' ESCAPE '|'
 * ```
 * 
 * The escape sequence should be one character long to specify the escape character, or empty 
 * to specify that no escape character is used. The expression must evaluate as a constant at 
 * execution time. If the 
 * [`NO_BACKSLASH_ESCAPES`](https://dev.mysql.com/doc/refman/8.0/en/sql-mode.html#sqlmode_no_backslash_escapes) 
 * SQL mode is enabled, the sequence cannot be empty.
 * 
 * ### Additional Information
 * 
 * The `pattern` need not be a literal string. For example, it can be specified as a string 
 * expression or table column. In the latter case, the column must be defined as one of the MySQL 
 * string types 
 * [(see Section 11.3, “String Data Types”)](https://dev.mysql.com/doc/refman/8.0/en/string-types.html).
 * 
 * Per the SQL standard, LIKE performs matching on a per-character basis, thus it can produce results 
 * different from the = comparison operator:
 * 
 * ```SQL
 * mysql> SELECT 'ä' LIKE 'ae' COLLATE latin1_german2_ci;
 *        -> 0
 * mysql> SELECT 'ä' = 'ae' COLLATE latin1_german2_ci;
 *        -> 1
 * ```
 * 
 * In particular, trailing spaces are always significant. This differs from comparisons performed 
 * with the `=` operator, for which the significance of trailing spaces in nonbinary strings (`CHAR`, `VARCHAR`, 
 * and `TEXT` values) depends on the pad attribute of the collation used for the comparison. For more 
 * information, see 
 * [Trailing Space Handling in Comparisons](https://dev.mysql.com/doc/refman/8.0/en/charset-binary-collations.html#charset-binary-collations-trailing-space-comparisons).
 * 
 * The following two statements illustrate that string comparisons are not case-sensitive unless one of the operands 
 * is case-sensitive (uses a case-sensitive collation or is a binary string):
 * 
 * ```SQL
 * mysql> SELECT 'abc' LIKE 'ABC';
 *         -> 1
 * mysql> SELECT 'abc' LIKE _utf8mb4 'ABC' COLLATE utf8mb4_0900_as_cs;
 *         -> 0
 * mysql> SELECT 'abc' LIKE _utf8mb4 'ABC' COLLATE utf8mb4_bin;
 *         -> 0
 * mysql> SELECT 'abc' LIKE BINARY 'ABC';
 *         -> 0
 * ```
 * 
 */
export function like(expr: Arg, pattern: Arg<string>, args?: { escape: Arg<string> }): Col<boolean> {
  return new Col({
    defer(q, context) {
      let str = `${q.colRef(expr, context)} LIKE ${q.colRef(pattern, context)}`;

      if (args?.escape)
        str += ` ESCAPE ${q.colRef(args.escape)}`;

      return str;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-comparison-functions.html#operator_not-like
 * 
 * Equivalent to: `NOT (expr LIKE pattern)`;
 * 
 * See also: 
 * - {@link like}
 */
export function not_like(expr: Arg, pattern: Arg<string>, args?: { escape: Arg<string> }): Col<boolean> {
  return new Col({
    defer(q, context) {
      let str = `${q.colRef(expr, context)} NOT LIKE ${q.colRef(pattern, context)}`;

      if (args?.escape)
        str += ` ESCAPE ${q.colRef(args.escape)}`;

      return str;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/string-comparison-functions.html#function_strcmp
 * 
 * `STRCMP()` returns `0` if the strings are the same, `-1` if the first argument is smaller than 
 * the second according to the current sort order, and `NULL` if either argument is `NULL`. 
 * It returns `1` otherwise.
 * 
 * ```SQL
 * mysql> SELECT STRCMP('text', 'text2');
 *         -> -1
 * mysql> SELECT STRCMP('text2', 'text');
 *         -> 1
 * mysql> SELECT STRCMP('text', 'text');
 *         -> 0
 * ```
 * 
 * `STRCMP()` performs the comparison using the collation of the arguments.
 * 
 * ```SQL
 * mysql> SET @s1 = _utf8mb4 'x' COLLATE utf8mb4_0900_ai_ci;
 * mysql> SET @s2 = _utf8mb4 'X' COLLATE utf8mb4_0900_ai_ci;
 * mysql> SET @s3 = _utf8mb4 'x' COLLATE utf8mb4_0900_as_cs;
 * mysql> SET @s4 = _utf8mb4 'X' COLLATE utf8mb4_0900_as_cs;
 * mysql> SELECT STRCMP(@s1, @s2), STRCMP(@s3, @s4);
 *        -> 0, 1
 * ```
 * 
 * If the collations are incompatible, one of the arguments must be converted to be compatible with 
 * the other. See 
 * [Section 10.8.4, “Collation Coercibility in Expressions”](https://dev.mysql.com/doc/refman/8.0/en/charset-collation-coercibility.html).
 * 
 * ```SQL
 * mysql> SET @s1 = _utf8mb4 'x' COLLATE utf8mb4_0900_ai_ci;
 * mysql> SET @s2 = _utf8mb4 'X' COLLATE utf8mb4_0900_ai_ci;
 * mysql> SET @s3 = _utf8mb4 'x' COLLATE utf8mb4_0900_as_cs;
 * mysql> SET @s4 = _utf8mb4 'X' COLLATE utf8mb4_0900_as_cs;
 * 
 * mysql> SELECT STRCMP(@s1, @s3);
 *        -- ERROR 1267 (HY000): Illegal mix of collations (utf8mb4_0900_ai_ci,IMPLICIT) and (utf8mb4_0900_as_cs,IMPLICIT) for operation 'strcmp'
 * 
 * mysql> SELECT STRCMP(@s1, @s3 COLLATE utf8mb4_0900_ai_ci);
 *        -> 0
 * ```
 */
export function strcmp(expr1: Arg<string>, expr2: Arg<string>): Col<boolean> {
  return new Col({
    defer(q, context) {
      return `STRCMP(${q.colRef(expr1, context)}, ${q.colRef(expr2, context)})`;
    }
  });
}

// #endregion

// #region FLOW CONTROL FUNCTIONS

export type CaseArg<T = any, U = any> = { when: Arg<T>; then: Arg<U>; else?: Arg<U>; }

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/flow-control-functions.html#operator_case
 * 
 * The first `CASE` syntax returns the `result` for the first `value=compare_value` comparison 
 * that is true. The second syntax returns the result for the first condition that is true. 
 * If no comparison or condition is true, the result after `ELSE` is returned, or `NULL` if 
 * there is no `ELSE` part.
 * 
 * ```SQL
 * mysql> SELECT CASE 1 
 *          WHEN 1 THEN 'one'
 *          WHEN 2 THEN 'two' 
 *          ELSE 'more' 
 *          END;
 *         -> 'one'
 * mysql> SELECT CASE WHEN 1 > 0 THEN 'true' ELSE 'false' END;
 *         -> 'true'
 * mysql> SELECT CASE BINARY 'B' WHEN 'a' THEN 1 WHEN 'b' THEN 2 END;
 *         -> NULL
 * ```
 * 
 * ### Usage
 * 
 * ```typescript
 * import { ccase, greaterThan } from 'mysql-query-builder';
 * 
 * ccase(1, { when: 1, then: 'one' }, { when: 2, then: 'two', else: 'more' }); // => Col<string> -> CASE 1 WHEN 1 THEN 'one' WHEN 2 THEN 'two' ELSE 'more' END
 * 
 * ccase({ when: greaterThan(1, 0), then: 'true', else: 'false' }) // => Col<string> -> CASE WHEN 1 > 0 THEN 'true' ELSE 'false' END
 * ```
 * 
 * ### Remarks
 * 
 * The return type of a CASE expression result is the aggregated type of all result values:
 * - If all types are numeric, the aggregated type is also numeric:
 *     - If at least one argument is double precision, the result is double precision.
 *     - Otherwise, if at least one argument is `DECIMAL`, the result is `DECIMAL`.
 *     - Otherwise, the result is an integer type (with one exception):
 *         - If all integer types are all signed or all unsigned, the result is the 
 *           same sign and the precision is the highest of all specified integer types 
 *           (that is, `TINYINT`, `SMALLINT`, `MEDIUMINT`, `INT`, or `BIGINT`).
 *         - If there is a combination of signed and unsigned integer types, the result 
 *           is signed and the precision may be higher. For example, if the types are 
 *           signed `INT` and unsigned `INT`, the result is signed `BIGINT`.
 *         - The exception is unsigned `BIGINT` combined with any signed integer type. 
 *           The result is `DECIMAL` with sufficient precision and scale `0`.
 * - If all types are `BIT`, the result is `BIT`. Otherwise, `BIT` arguments are treated similar 
 *   to `BIGINT`.
 * - If all types are `YEAR`, the result is `YEAR`. Otherwise, `YEAR` arguments are treated 
 *   similar to `INT`.
 * - If all types are character string (`CHAR` or `VARCHAR`), the result is `VARCHAR` with maximum 
 *   length determined by the longest character length of the operands.
 * - If all types are character or binary string, the result is `VARBINARY`.
 * - `SET` and `ENUM` are treated similar to `VARCHAR`; the result is `VARCHAR`.
 * - If all types are `JSON`, the result is `JSON`.
 * - If all types are temporal, the result is temporal:
 *     - If all temporal types are `DATE`, `TIME`, or `TIMESTAMP`, the result is `DATE`, `TIME`, or 
 *       `TIMESTAMP`, respectively.
 *     - Otherwise, for a mix of temporal types, the result is `DATETIME`.
 * - If all types are `GEOMETRY`, the result is `GEOMETRY`.
 * - If any type is `BLOB`, the result is `BLOB`.
 * - For all other type combinations, the result is `VARCHAR`.
 * - Literal `NULL` operands are ignored for type aggregation.
 * 
 * ### Notes
 * > The syntax of the `CASE` operator described here differs slightly from that of the SQL `CASE` statement 
 * > described in 
 * > [Section 13.6.5.1, “CASE Statement”](https://dev.mysql.com/doc/refman/8.0/en/case.html), 
 * > for use inside stored programs. The `CASE` statement cannot have an `ELSE NULL` clause, and it is 
 * > terminated with `END CASE` instead of `END`.
 */
export function ccase<T, U>(target: Arg<T>, ...args: CaseArg<T, U>[]): Col<U>;
export function ccase<T, U>(...args: CaseArg<T, U>[]): Col<U>;
export function ccase<T, U>(...args: any[]): Col<U> {

  const target: Arg<T> | undefined = (() => {
    for (const key in args[0])
      switch (key as keyof CaseArg) {
        case 'else':
        case 'then':
        case 'when':
          return undefined;
      }

    return args[0];
  })();

  const arg_index = target === undefined ? 0 : 1;

  return new Col({
    defer(q, context) {

      let when_thens = '';

      for (let i = arg_index; i < args.length; i++) {
        const arg = args[i];

        const when = q.colRef(arg.when, context);
        const then = q.colRef(arg.then, context);

        let str = `WHEN ${when} THEN ${then}`;

        if (arg.else)
          str += ` ELSE ${q.colRef(arg.else, context)}`;

        when_thens += when_thens ? `\r\n${str}` : str;
      }

      if (target === undefined)
        return `CASE ${when_thens} END`;

      else
        return `CASE ${q.colRef(target, context)} ${when_thens} END`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/flow-control-functions.html#function_if
 * 
 * If `expr1` is `TRUE` (`expr1` <> 0 and `expr1` `IS NOT NULL`), `IF()` returns `expr2`. Otherwise, it returns `expr3`.
 * 
 * ```SQL
 * mysql> SELECT IF(1 > 2, 2, 3);
 *         -> 3
 * mysql> SELECT IF(1 < 2, 'yes', 'no');
 *         -> 'yes'
 * mysql> SELECT IF(STRCMP('test', 'test1'), 'no', 'yes');
 *         -> 'no'
 * ```
 */
export function iif<T, U>(target: Arg<boolean>, then: Arg<T>, _else: Arg<U>) {
  return new Col<T | U>({
    defer(q, context) {
      return `IF(${q.colRef(target, context)}, ${q.colRef(then, context)}, ${q.colRef(_else, context)})`;
    }
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/flow-control-functions.html#function_nullif
 * 
 * Returns `NULL` if `expr1 = expr2` is true, otherwise returns `expr1`. This is the same as 
 * `CASE WHEN expr1 = expr2 THEN NULL ELSE expr1 END`.
 * 
 * The return value has the same type as the first argument.
 * 
 * ```SQL
 * mysql> SELECT NULLIF(1,1);
 *         -> NULL
 * mysql> SELECT NULLIF(1,2);
 *         -> 1
 * ```
 * 
 * **Note:** 
 * MySQL evaluates `expr1` twice if the arguments are not equal.
 */
export function nullif<T>(expr1: Arg<T>, expr2: Arg<T>) {
  return new Col<T | null>({
    defer(q, context) {
      return `NULLIF(${q.colRef(expr1, context)}, ${q.colRef(expr2, context)})`;
    }
  });
}

// #endregion

// #region BIT OPERATIONS

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/bit-functions.html#operator_bitwise-and
 * 
 * The result type depends on whether the arguments are evaluated as binary strings 
 * or numbers:
 * - Binary-string evaluation occurs when the arguments have a binary string type, 
 * and at least one of them is not a hexadecimal literal, bit literal, or `NULL` literal. 
 * Numeric evaluation occurs otherwise, with argument conversion to unsigned 64-bit 
 * integers as necessary.
 * - Binary-string evaluation produces a binary string of the same length as the arguments. 
 * If the arguments have unequal lengths, an 
 * [ER_INVALID_BITWISE_OPERANDS_SIZE](https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_invalid_bitwise_operands_size) 
 * error occurs. Numeric evaluation produces an unsigned 64-bit integer.
 * 
 * ```SQL
 * mysql> SELECT 29 & 15;
 *         -> 13
 * mysql> SELECT HEX(_binary X'FF' & b'11110000');
 *         -> 'F0'
 * ```
 * If `bitwise AND` is invoked from within the mysql client, binary strings display using hexadecimal 
 * notation, depending on the value of the 
 * [`--binary-as-hex`](https://dev.mysql.com/doc/refman/8.0/en/mysql-command-options.html#option_mysql_binary-as-hex). 
 * For more information about that option, see 
 * [Section 4.5.1, “mysql — The MySQL Command-Line Client”](https://dev.mysql.com/doc/refman/8.0/en/mysql.html). 
 */
export function bitwise_and(...args: Arg[]) {
  return new Col({
    defer: (q, context) => args.reduce<string>((prev, arg) => {
      const ref = q.colRef(arg, context);
      return prev ? `${prev} & ${ref}` : ref;
    }, '')
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/bit-functions.html#operator_right-shift
 * 
 * Shifts a longlong (`BIGINT`) number or binary string to the right.
 * 
 * The result type depends on whether the arguments are evaluated as binary strings 
 * or numbers:
 * - Binary-string evaluation occurs when the arguments have a binary string type, 
 * and at least one of them is not a hexadecimal literal, bit literal, or `NULL` literal. 
 * Numeric evaluation occurs otherwise, with argument conversion to unsigned 64-bit 
 * integers as necessary.
 * - Binary-string evaluation produces a binary string of the same length as the arguments. 
 * If the arguments have unequal lengths, an 
 * [ER_INVALID_BITWISE_OPERANDS_SIZE](https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_invalid_bitwise_operands_size) 
 * error occurs. Numeric evaluation produces an unsigned 64-bit integer.
 * 
 * ```SQL
 * mysql> SELECT 4 >> 2;
 *         -> 1
 * mysql> SELECT HEX(_binary X'00FF00FF00FF' >> 8);
 *         -> '0000FF00FF00'
 * ```
 * If a bitshift is invoked from within the mysql client, binary strings display using hexadecimal 
 * notation, depending on the value of the 
 * [`--binary-as-hex`](https://dev.mysql.com/doc/refman/8.0/en/mysql-command-options.html#option_mysql_binary-as-hex). 
 * For more information about that option, see 
 * [Section 4.5.1, “mysql — The MySQL Command-Line Client”](https://dev.mysql.com/doc/refman/8.0/en/mysql.html). 
 */
export function bitwise_shift_right(...args: Arg[]) {
  return new Col({
    defer: (q, context) => args.reduce<string>((prev, arg) => {
      const ref = q.colRef(arg, context);
      return prev ? `${prev} >> ${ref}` : ref;
    }, '')
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/bit-functions.html#operator_left-shift
 * 
 * Shifts a longlong (`BIGINT`) number or binary string to the left.
 * 
 * The result type depends on whether the arguments are evaluated as binary strings 
 * or numbers:
 * - Binary-string evaluation occurs when the arguments have a binary string type, 
 * and at least one of them is not a hexadecimal literal, bit literal, or `NULL` literal. 
 * Numeric evaluation occurs otherwise, with argument conversion to unsigned 64-bit 
 * integers as necessary.
 * - Binary-string evaluation produces a binary string of the same length as the arguments. 
 * If the arguments have unequal lengths, an 
 * [ER_INVALID_BITWISE_OPERANDS_SIZE](https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_invalid_bitwise_operands_size) 
 * error occurs. Numeric evaluation produces an unsigned 64-bit integer.
 * 
 * ```SQL
 * mysql> SELECT 1 << 2;
 *         -> 4
 * mysql> SELECT HEX(_binary X'00FF00FF00FF' << 8);
 *         -> 'FF00FF00FF00'
 * ```
 * If a bitshift is invoked from within the mysql client, binary strings display using hexadecimal 
 * notation, depending on the value of the 
 * [`--binary-as-hex`](https://dev.mysql.com/doc/refman/8.0/en/mysql-command-options.html#option_mysql_binary-as-hex). 
 * For more information about that option, see 
 * [Section 4.5.1, “mysql — The MySQL Command-Line Client”](https://dev.mysql.com/doc/refman/8.0/en/mysql.html). 
 */
export function bitwise_shift_left(...args: Arg[]) {
  return new Col({
    defer: (q, context) => args.reduce<string>((prev, arg) => {
      const ref = q.colRef(arg, context);
      return prev ? `${prev} << ${ref}` : ref;
    }, '')
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/bit-functions.html#operator_bitwise-or
 * 
 * The result type depends on whether the arguments are evaluated as binary strings 
 * or numbers:
 * - Binary-string evaluation occurs when the arguments have a binary string type, 
 * and at least one of them is not a hexadecimal literal, bit literal, or `NULL` literal. 
 * Numeric evaluation occurs otherwise, with argument conversion to unsigned 64-bit 
 * integers as necessary.
 * - Binary-string evaluation produces a binary string of the same length as the arguments. 
 * If the arguments have unequal lengths, an 
 * [ER_INVALID_BITWISE_OPERANDS_SIZE](https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_invalid_bitwise_operands_size) 
 * error occurs. Numeric evaluation produces an unsigned 64-bit integer.
 * 
 * ```SQL
 * mysql> SELECT 29 | 15;
 *         -> 31
 * mysql> SELECT _binary X'40404040' | X'01020304';
 *         -> 'ABCD'
 * ```
 * If `bitwise OR` is invoked from within the mysql client, binary strings display using hexadecimal 
 * notation, depending on the value of the 
 * [`--binary-as-hex`](https://dev.mysql.com/doc/refman/8.0/en/mysql-command-options.html#option_mysql_binary-as-hex). 
 * For more information about that option, see 
 * [Section 4.5.1, “mysql — The MySQL Command-Line Client”](https://dev.mysql.com/doc/refman/8.0/en/mysql.html). 
 */
export function bitwise_or(...args: Arg[]) {
  return new Col({
    defer: (q, context) => args.reduce<string>((prev, arg) => {
      const ref = q.colRef(arg, context);
      return prev ? `${prev} | ${ref}` : ref;
    }, '')
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/bit-functions.html#operator_bitwise-xor
 * 
 * The result type depends on whether the arguments are evaluated as binary strings 
 * or numbers:
 * - Binary-string evaluation occurs when the arguments have a binary string type, 
 * and at least one of them is not a hexadecimal literal, bit literal, or `NULL` literal. 
 * Numeric evaluation occurs otherwise, with argument conversion to unsigned 64-bit 
 * integers as necessary.
 * - Binary-string evaluation produces a binary string of the same length as the arguments. 
 * If the arguments have unequal lengths, an 
 * [ER_INVALID_BITWISE_OPERANDS_SIZE](https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_invalid_bitwise_operands_size) 
 * error occurs. Numeric evaluation produces an unsigned 64-bit integer.
 * 
 * ```SQL
 * mysql> SELECT 1 ^ 1;
 *         -> 0
 * mysql> SELECT 1 ^ 0;
 *         -> 1
 * mysql> SELECT 11 ^ 3;
 *         -> 8
 * mysql> SELECT HEX(_binary X'FEDC' ^ X'1111');
 *         -> 'EFCD'
 * ```
 * If a bitwise `XOR` is invoked from within the mysql client, binary strings display using hexadecimal 
 * notation, depending on the value of the 
 * [`--binary-as-hex`](https://dev.mysql.com/doc/refman/8.0/en/mysql-command-options.html#option_mysql_binary-as-hex). 
 * For more information about that option, see 
 * [Section 4.5.1, “mysql — The MySQL Command-Line Client”](https://dev.mysql.com/doc/refman/8.0/en/mysql.html). 
 */
export function bitwise_xor(...args: Arg[]) {
  return new Col({
    defer: (q, context) => args.reduce<string>((prev, arg) => {
      const ref = q.colRef(arg, context);
      return prev ? `${prev} ^ ${ref}` : ref;
    }, '')
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/bit-functions.html#operator_bitwise-invert
 * 
 * Invert all bits.
 * 
 * The result type depends on whether the arguments are evaluated as binary strings 
 * or numbers:
 * - Binary-string evaluation occurs when the arguments have a binary string type, 
 * and at least one of them is not a hexadecimal literal, bit literal, or `NULL` literal. 
 * Numeric evaluation occurs otherwise, with argument conversion to unsigned 64-bit 
 * integers as necessary.
 * - Binary-string evaluation produces a binary string of the same length as the arguments. 
 * If the arguments have unequal lengths, an 
 * [ER_INVALID_BITWISE_OPERANDS_SIZE](https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_invalid_bitwise_operands_size) 
 * error occurs. Numeric evaluation produces an unsigned 64-bit integer.
 * 
 * ```SQL
 * mysql> SELECT 5 & ~1;
 *         -> 4
 * mysql> SELECT HEX(~X'0000FFFF1111EEEE');
 *         -> 'FFFF0000EEEE1111'
 * ```
 * If a bitwise inversion is invoked from within the mysql client, binary strings display using hexadecimal 
 * notation, depending on the value of the 
 * [`--binary-as-hex`](https://dev.mysql.com/doc/refman/8.0/en/mysql-command-options.html#option_mysql_binary-as-hex). 
 * For more information about that option, see 
 * [Section 4.5.1, “mysql — The MySQL Command-Line Client”](https://dev.mysql.com/doc/refman/8.0/en/mysql.html). 
 */
export function bitwise_invert(...args: Arg[]): Col<any> {
  return new Col({
    defer: (q, context) => args.reduce<string>((prev, arg) => {
      const ref = q.colRef(arg, context);
      return prev ? `${prev} ~ ${ref}` : ref;
    }, '')
  });
}

/**
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/bit-functions.html#function_bit-count
 * 
 * Returns the number of bits that are set in the argument N as an unsigned 64-bit integer, or NULL if the argument is NULL
 * 
 * ```SQL
 * mysql> SELECT BIT_COUNT(64), BIT_COUNT(BINARY 64);
 *         -> 1, 7
 * mysql> SELECT BIT_COUNT('64'), BIT_COUNT(_binary '64');
 *         -> 1, 7
 * mysql> SELECT BIT_COUNT(X'40'), BIT_COUNT(_binary X'40');
 *         -> 1, 1
 * ```
 */
export function bit_count(arg: Arg<string | number>): Col<number> {
  return new Col({
    defer: (q, context) => `BIT_COUNT(${q.colRef(arg, context)})`
  });
}

//#endregion

// #region TYPE CONVERSIONS

export type DataType = 'BINARY' | 'CHAR' | 'DATE' | 'DATETIME' | 'DECIMAL' | 'DOUBLE' | 'FLOAT' | 'JSON' | 'NCHAR' | 'REAL' | 'SIGNED' | 'TIME' | 'UNSIGNED' | 'YEAR';

export function cast(arg: Arg, opt: { as: DataType }) {
  return new Col({
    defer: (q, context) => `CAST(${q.colRef(arg, context)} AS ${opt.as})`
  });
}

export function convert(arg: Arg, opt: { using: string } | DataType) {
  return new Col({
    defer: (q, context) => {
      const ref = q.colRef(arg, context);
      if (typeof opt === 'string')
        return `CONVERT(${ref}, ${opt})`;
      else
        return `CONVERT(${ref} USING ${opt.using})`;
    }
  });
}

// #endregion



type MatchSearchModifiers = 'IN NATURAL LANGUAGE MODE' | 'IN NATURAL LANGUAGE MODE WITH QUERY EXPANSION' | 'IN BOOLEAN MODE' | 'WITH QUERY EXPANSION';

export function match(cols: Col<string>[], opts: { against: Arg<string>; in?: MatchSearchModifiers & string }) {
  return new Col<number>({
    defer: (q, ctx) => {

      let against = q.colRef(opts.against, ctx);

      if (opts.in)
        against += ` ${opts.in}`;

      return `MATCH (${cols.map(col => q.colRef(col, ctx)).join(', ')}) AGAINST (${against})`;
    }
  });
}


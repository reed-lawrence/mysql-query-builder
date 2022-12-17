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
    else if (typeof obj === 'string' || typeof obj === 'number' || typeof obj === 'boolean')
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

export function isEqualTo<T>(target: Arg<T>, value: Arg<T>, args?: { null_safe: boolean }): Col<boolean>;
export function isEqualTo(target: Arg<boolean | 1 | 0>, value: Arg<boolean | 1 | 0>, args?: { null_safe: boolean }): Col<boolean>;
export function isEqualTo<T>(target: Arg<T>, value: Arg<T>, args?: { null_safe: boolean }): Col<boolean> {
  return _equalityOp(target, value, args?.null_safe ? '<=>' : '=');
}

export function isGreaterThanOrEqualTo<T>(target: Arg<T>, value: Arg<T>) {
  return _equalityOp(target, value, '>=');
}

export function isLessThanOrEqualTo<T>(target: Arg<T>, value: Arg<T>) {
  return _equalityOp(target, value, '<=');
}

export function isNotEqualTo<T>(target: Arg<T>, value: Arg<T>) {
  return _equalityOp(target, value, '<>');
}

export function isGreaterThan<T>(target: Arg<T>, value: Arg<T>) {
  return _equalityOp(target, value, '>');
}

export function isLessThan<T>(target: Arg<T>, value: Arg<T>) {
  return _equalityOp(target, value, '<');
}

//#endregion

//#region Artihmatic Operations

function _arithmaticOp(target: Arg<number>, value: Arg<number>, symbol: '+' | '-' | '/' | '*' | '%' | 'DIV') {
  return new Col<number>({ defer: (q, ctx) => `(${q.colRef(target, ctx)} ${symbol} ${q.colRef(value, ctx)})` });
}

export function add(target: Arg<number>, value: Arg<number>) {
  return _arithmaticOp(target, value, '+');
}

export function subtract(target: Arg<number>, value: Arg<number>) {
  return _arithmaticOp(target, value, '-');
}

export function divide(target: Arg<number>, value: Arg<number>, { integer = false }) {
  return _arithmaticOp(target, value, integer ? 'DIV' : '/');
}

export function multiply(target: Arg<number>, value: Arg<number>) {
  return _arithmaticOp(target, value, '*');
}

export function modulo(target: Arg<number>, value: Arg<number>) {
  return _arithmaticOp(target, value, '%');
}

export function abs(value: Arg<number>) {
  return new Col<number>({ defer: (q, ctx) => `ABS(${q.colRef(value, ctx)})` });
}

export function acos(value: Arg<number>) {
  return new Col<number>({ defer: (q, ctx) => `ACOS(${q.colRef(value, ctx)})` });
}

export function asin(value: Arg<number>) {
  return new Col<number>({ defer: (q, ctx) => `ASIN(${q.colRef(value, ctx)})` });
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

export function format(value: Arg<number>, arg: Arg<number>, locale?: Arg<string & 'en_US'>) {
  if (locale)
    return new Col<number>({ defer: (q, ctx) => `FORMAT(${q.colRef(value, ctx)}, ${q.colRef(arg, ctx)}, ${q.colRef(locale, ctx)})` });
  else
    return new Col<number>({ defer: (q, ctx) => `FORMAT(${q.colRef(value, ctx)}, ${q.colRef(arg, ctx)})` });
}

export function hex(value: Arg<string | number>) {
  return new Col<string>({
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

//#endregion

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

export function is(target: Arg, value: Arg | null) {
  return new Col<boolean>({
    defer(q, ctx) {
      if (value === null)
        return `${q.colRef(target, ctx)} IS NULL`;

      else
        return `${q.colRef(target, ctx)} IS NOT ${q.colRef(value, ctx)}`;
    },
  })
}

export function is_not(target: Arg, value: Arg | null) {
  return new Col<boolean>({
    defer(q, ctx) {
      if (value === null)
        return `${q.colRef(target, ctx)} IS NOT NULL`;

      else
        return `${q.colRef(target, ctx)} IS NOT ${q.colRef(value, ctx)}`;
    }
  });
}

export function is_null(target: Arg) {
  return new Col<boolean>({
    defer(q, context) {
      return `IS_NULL(${q.colRef(target, context)})`;
    },
  })
}

export function if_null<T>(target: Arg<unknown>, value: Arg<T>) {
  return operation<T>((q, ctx) => `IFNULL(${q.colRef(target, ctx)}, ${q.colRef(value, ctx)})`);
}

export function and(...args: Col<boolean>[]) {
  return operation<boolean>((q, ctx) => `(${args.map(op => q.colRef(op, ctx)).join(' AND ')})`);
}

export function or(...args: Col<boolean>[]) {
  return operation<boolean>((q, ctx) => `(${args.map(op => q.colRef(op, ctx)).join(' OR ')})`);
}

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

export function between<T>(target: Arg<T>, start: Arg<T>, end: Arg<T>) {
  return new Col<boolean>({
    defer(q, context) {
      return `${q.colRef(target, context)} BETWEEN ${q.colRef(start, context)} AND ${q.colRef(end, context)}`;
    }
  });
}

export function not_between<T>(target: Arg<T>, start: Arg<T>, end: Arg<T>) {
  return new Col<boolean>({
    defer(q, context) {
      return `NOT (${q.colRef(target, context)} BETWEEN ${q.colRef(start, context)} AND ${q.colRef(end, context)})`;
    }
  });
}

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

// #region FLOW CONTROL FUNCTIONS

export type CaseArg<T, U> = { when: Arg<T>; then: Arg<U>; else?: Arg<U>; }
export function ccase<T, U>(target: Arg<T>, ...args: CaseArg<T, U>[]) {
  return new Col<U>({
    defer(q, context) {
      const when_thens = args.reduce((out, arg) => {

        const when = q.colRef(arg.when, context);
        const then = q.colRef(arg.then, context);

        let str = `WHEN ${when} THEN ${then}`;

        if (arg.else)
          str += ` ELSE ${q.colRef(arg.else, context)}`;

        return out ? `\r\n${str}` : str;
      }, '');
      return `CASE ${q.colRef(target, context)} ${when_thens} END`;
    }
  });
}

export function iif<T, U>(target: Arg<boolean>, then: Arg<T>, _else: Arg<U>) {
  return new Col<T | U>({
    defer(q, context) {
      return `IF(${q.colRef(target, context)}, ${q.colRef(then, context)}, ${q.colRef(_else, context)})`;
    }
  });
}

export function null_if<T>(expr1: Arg<T>, expr2: Arg<T>) {
  return new Col<T | null>({
    defer(q, context) {
      return `NULLIF(${q.colRef(expr1, context)}, ${q.colRef(expr2, context)})`;
    }
  });
}

// #endregion

// #region BIT OPERATIONS

export function bitwise_and(...args: Arg[]) {
  return new Col({
    defer: (q, context) => args.reduce<string>((prev, arg) => {
      const ref = q.colRef(arg, context);
      return prev ? `${prev} & ${ref}` : ref;
    }, '')
  });
}

export function bitwise_shift_right(...args: Arg[]) {
  return new Col({
    defer: (q, context) => args.reduce<string>((prev, arg) => {
      const ref = q.colRef(arg, context);
      return prev ? `${prev} >> ${ref}` : ref;
    }, '')
  });
}

export function bitwise_shift_left(...args: Arg[]) {
  return new Col({
    defer: (q, context) => args.reduce<string>((prev, arg) => {
      const ref = q.colRef(arg, context);
      return prev ? `${prev} << ${ref}` : ref;
    }, '')
  });
}

export function bitwise_or(...args: Arg[]) {
  return new Col({
    defer: (q, context) => args.reduce<string>((prev, arg) => {
      const ref = q.colRef(arg, context);
      return prev ? `${prev} | ${ref}` : ref;
    }, '')
  });
}

export function bitwise_xor(...args: Arg[]) {
  return new Col({
    defer: (q, context) => args.reduce<string>((prev, arg) => {
      const ref = q.colRef(arg, context);
      return prev ? `${prev} ^ ${ref}` : ref;
    }, '')
  });
}

export function bitwise_invert(...args: Arg[]) {
  return new Col({
    defer: (q, context) => args.reduce<string>((prev, arg) => {
      const ref = q.colRef(arg, context);
      return prev ? `${prev} ~ ${ref}` : ref;
    }, '')
  });
}

export function bit_count(arg: Arg<string | number>) {
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


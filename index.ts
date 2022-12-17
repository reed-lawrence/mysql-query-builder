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

type ExpressionArg<T = unknown> = QCol<T> | T;

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

export class QCol<ColType = any, TParent extends object = any> {

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

function operation<T>(fn: NonNullable<QCol<T>['defer']>) {
  return new QCol<T>({ defer: fn });
}

export function subquery<T>(value: QSubquery<T>) {
  return new QCol<T>({
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

export type QColMap<T> = { [Index in keyof T]: QCol<T[Index]> }
type QExpressionMap<T> = { [Index in keyof T]: QCol<T[Index]> | T[Index] }

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
        prev[key as keyof QColMap<T>] = new QCol({ path: key, parent: this });
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
  selected: Record<string, QCol> = {};
  wheres: QCol[] = [];
  havings: QCol[] = [];
  joins: QCol[] = [];
  unions: { type?: 'ALL', op: QCol }[] = [];
  groupBys: QCol[] = [];
  orderBys: OrderByArgDirectional[] = [];

  limit?: number;
  offset?: number;

  insert = {
    cols: [] as string[],
    values: [] as QCol[]
  }

  updates: [QCol, QCol][] = [];

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

  toCol<T>(obj: ExpressionArg<T>) {
    if (obj instanceof QCol)
      return obj;
    else if (typeof obj === 'string' || typeof obj === 'number' || typeof obj === 'boolean')
      return new QCol<T>({ path: this.paramaterize(obj) })
    else
      throw new Error(`Cannot convert ${JSON.stringify(obj)} to QCol`);
  }

  colRef<T = unknown>(arg: QCol<T> | ExpressionArg<T>, context: AccessContext = AccessContext.Default) {

    const col = arg instanceof QCol ? arg : this.toCol(arg);

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

type QColTuple<T> = [QCol<T>, T];
type ValidTuples = QColTuple<string> | QColTuple<number> | QCol<boolean> | [QCol<number>, QCol<number>] | [QCol<string>, QCol<string>] | [QCol<boolean>, QCol<boolean>];

type OrderByArgDirectional = { direction: 'asc' | 'desc'; col: QCol; }
export type OrderByArg = QCol | OrderByArgDirectional | (QCol | OrderByArgDirectional)[];

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
    t1On: (model: T) => QCol<TKey>,
    t2On: (model: QColMap<J>) => QCol<TKey>,
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

  innerJoin<J extends object, TKey, U extends Record<string, QColMap<unknown>>>(toJoin: QSubquery<J>, t1On: (model: T) => QCol<TKey>, t2On: (model: J) => QCol<TKey>, join: (tModel: T, joined: J) => U): QJoined<U, TSelected, BaseType>;
  innerJoin<J extends object, TKey, U extends Record<string, QColMap<unknown>>>(toJoin: Table<J> | QSubquery<J>, t1On: (model: T) => QCol<TKey>, t2On: (model: QColMap<J>) => QCol<TKey>, join: (tModel: T, joined: QColMap<J>) => U): QJoined<U, TSelected, BaseType> {
    return this._join('INNER', toJoin, t1On, t2On, join);
  }

  rightJoin<J extends object, TKey, U extends Record<string, QColMap<unknown>>>(toJoin: QSubquery<J>, t1On: (model: T) => QCol<TKey>, t2On: (model: J) => QCol<TKey>, join: (tModel: T, joined: J) => U): QJoined<U, TSelected, BaseType>;
  rightJoin<J extends object, TKey, U extends Record<string, QColMap<unknown>>>(toJoin: Table<J> | QSubquery<J>, t1On: (model: T) => QCol<TKey>, t2On: (model: QColMap<J>) => QCol<TKey>, join: (tModel: T, joined: QColMap<J>) => U): QJoined<U, TSelected, BaseType> {
    return this._join('RIGHT', toJoin, t1On, t2On, join);
  }

  leftJoin<J extends object, TKey, U extends Record<string, QColMap<unknown>>>(toJoin: QSubquery<J>, t1On: (model: T) => QCol<TKey>, t2On: (model: J) => QCol<TKey>, join: (tModel: T, joined: J) => U): QJoined<U, TSelected, BaseType>;
  leftJoin<J extends object, TKey, U extends Record<string, QColMap<unknown>>>(toJoin: Table<J> | QSubquery<J>, t1On: (model: T) => QCol<TKey>, t2On: (model: QColMap<J>) => QCol<TKey>, join: (tModel: T, joined: QColMap<J>) => U): QJoined<U, TSelected, BaseType> {
    return this._join('LEFT', toJoin, t1On, t2On, join);
  }

  crossJoin<J extends object, TKey, U extends Record<string, QColMap<unknown>>>(toJoin: QSubquery<J>, t1On: (model: T) => QCol<TKey>, t2On: (model: J) => QCol<TKey>, join: (tModel: T, joined: J) => U): QJoined<U, TSelected, BaseType>;
  crossJoin<J extends object, TKey, U extends Record<string, QColMap<unknown>>>(toJoin: Table<J> | QSubquery<J>, t1On: (model: T) => QCol<TKey>, t2On: (model: QColMap<J>) => QCol<TKey>, join: (tModel: T, joined: QColMap<J>) => U): QJoined<U, TSelected, BaseType> {
    return this._join('CROSS', toJoin, t1On, t2On, join);
  }

  select<U extends Record<string, QCol>>(fn: (model: T) => U): QSelected<T, U> {
    const selected = fn(this.model);
    this.q.selected = selected;
    return new QBase(this.model, this.q, selected, this.q_type);
  }

  set(fn: (model: T) => ValidTuples[]): QUpdated<T, T> {

    const tuples = fn(this.model) as [QCol, ExpressionArg<unknown>][];

    for (const pair of tuples)
      this.q.updates.push([pair[0], this.q.toCol(pair[1])]);

    return new QBase(this.model, this.q, this.model, this.q_type);
  }

  where(fn: (model: TSelected, data: T) => QCol): QWhere<T, TSelected> {
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

  having(fn: (model: TSelected, data: T) => QCol): QHaving<T, TSelected> {
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

  groupBy(fn: (model: TSelected, data: T) => QCol | QCol[]): QGrouped<T, TSelected, BaseType> {
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

      let col: QCol;

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

        col = new QCol({
          defer: (q, ctx) => this.q.insert.cols.map((key) => {

            const val = (value as Record<string, any>)[key] as ExpressionArg<unknown>;

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

export function count(value: QCol) {
  return operation<number>((q, ctx) => `COUNT(${q.colRef(value, ctx)})`);
}

// #endregion

// #region Equality Operations

function _equalityOp<T>(target: ExpressionArg<T>, value: ExpressionArg<T>, symbol: '=' | '<>' | '<=' | '>=' | '<' | '>' | '<=>') {
  return new QCol<boolean>({ defer: (q, ctx) => `${q.colRef(target, ctx)} ${symbol} ${q.colRef(value, ctx)}` });
}

export function isEqualTo<T>(target: ExpressionArg<T>, value: ExpressionArg<T>, args?: { null_safe: boolean }): QCol<boolean>;
export function isEqualTo(target: ExpressionArg<boolean | 1 | 0>, value: ExpressionArg<boolean | 1 | 0>, args?: { null_safe: boolean }): QCol<boolean>;
export function isEqualTo<T>(target: ExpressionArg<T>, value: ExpressionArg<T>, args?: { null_safe: boolean }): QCol<boolean> {
  return _equalityOp(target, value, args?.null_safe ? '<=>' : '=');
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

function _arithmaticOp(target: ExpressionArg<number>, value: ExpressionArg<number>, symbol: '+' | '-' | '/' | '*' | '%' | 'DIV') {
  return new QCol<number>({ defer: (q, ctx) => `(${q.colRef(target, ctx)} ${symbol} ${q.colRef(value, ctx)})` });
}

export function add(target: ExpressionArg<number>, value: ExpressionArg<number>) {
  return _arithmaticOp(target, value, '+');
}

export function subtract(target: ExpressionArg<number>, value: ExpressionArg<number>) {
  return _arithmaticOp(target, value, '-');
}

export function divide(target: ExpressionArg<number>, value: ExpressionArg<number>, { integer = false }) {
  return _arithmaticOp(target, value, integer ? 'DIV' : '/');
}

export function multiply(target: ExpressionArg<number>, value: ExpressionArg<number>) {
  return _arithmaticOp(target, value, '*');
}

export function modulo(target: ExpressionArg<number>, value: ExpressionArg<number>) {
  return _arithmaticOp(target, value, '%');
}

export function abs(value: ExpressionArg<number>) {
  return new QCol<number>({ defer: (q, ctx) => `ABS(${q.colRef(value, ctx)})` });
}

export function acos(value: ExpressionArg<number>) {
  return new QCol<number>({ defer: (q, ctx) => `ACOS(${q.colRef(value, ctx)})` });
}

export function asin(value: ExpressionArg<number>) {
  return new QCol<number>({ defer: (q, ctx) => `ASIN(${q.colRef(value, ctx)})` });
}

export function atan(value: ExpressionArg<number>, val2?: ExpressionArg<number>) {
  if (val2)
    return new QCol<number>({ defer: (q, ctx) => `ATAN(${q.colRef(value, ctx)}, ${q.colRef(val2, ctx)})` });
  else
    return new QCol<number>({ defer: (q, ctx) => `ATAN(${q.colRef(value, ctx)})` });
}

export function atan2(val1: ExpressionArg<number>, val2: ExpressionArg<number>) {
  return new QCol<number>({ defer: (q, ctx) => `ATAN2(${q.colRef(val1, ctx)}, ${q.colRef(val2, ctx)})` });
}

export function ceiling(value: ExpressionArg<number>) {
  return new QCol<number>({ defer: (q, ctx) => `CEILING(${q.colRef(value, ctx)})` });
}

export function conv(value: ExpressionArg<number | string>, from: ExpressionArg<number>, to: ExpressionArg<number>) {
  return new QCol<number>({ defer: (q, ctx) => `CONV(${q.colRef(value, ctx)}, ${q.colRef(from, ctx)}, ${q.colRef(to, ctx)})` });
}

export function cos(value: ExpressionArg<number>) {
  return new QCol<number>({ defer: (q, ctx) => `COS(${q.colRef(value, ctx)})` });
}

export function cot(value: ExpressionArg<number>) {
  return new QCol<number>({ defer: (q, ctx) => `COT(${q.colRef(value, ctx)})` });
}

export function crc32(value: ExpressionArg<number>) {
  return new QCol<number>({ defer: (q, ctx) => `CRC32(${q.colRef(value, ctx)})` });
}

export function degrees(value: ExpressionArg<number>) {
  return new QCol<number>({ defer: (q, ctx) => `DEGREES(${q.colRef(value, ctx)})` });
}

export function exp(value: ExpressionArg<number>) {
  return new QCol<number>({ defer: (q, ctx) => `EXP(${q.colRef(value, ctx)})` });
}

export function floor(value: ExpressionArg<number>) {
  return new QCol<number>({ defer: (q, ctx) => `FLOOR(${q.colRef(value, ctx)})` });
}

export function format(value: ExpressionArg<number>, arg: ExpressionArg<number>, locale?: ExpressionArg<string & 'en_US'>) {
  if (locale)
    return new QCol<number>({ defer: (q, ctx) => `FORMAT(${q.colRef(value, ctx)}, ${q.colRef(arg, ctx)}, ${q.colRef(locale, ctx)})` });
  else
    return new QCol<number>({ defer: (q, ctx) => `FORMAT(${q.colRef(value, ctx)}, ${q.colRef(arg, ctx)})` });
}

//#endregion

//#region Date Operations

export function timestamp(value: ExpressionArg<string> | Date) {
  return operation<string>((q, ctx) => {

    if (value instanceof Date)
      return `TIMESTAMP(${q.paramaterize(value.toISOString())})`;

    else
      return `TIMESTAMP(${q.colRef(value, ctx)})`;

  });
}

//#endregion

export function is(target: ExpressionArg, value: ExpressionArg | null) {
  return new QCol<boolean>({
    defer(q, ctx) {
      if (value === null)
        return `${q.colRef(target, ctx)} IS NULL`;

      else
        return `${q.colRef(target, ctx)} IS NOT ${q.colRef(value, ctx)}`;
    },
  })
}

export function is_not(target: ExpressionArg, value: ExpressionArg | null) {
  return new QCol<boolean>({
    defer(q, ctx) {
      if (value === null)
        return `${q.colRef(target, ctx)} IS NOT NULL`;

      else
        return `${q.colRef(target, ctx)} IS NOT ${q.colRef(value, ctx)}`;
    }
  });
}

export function is_null(target: ExpressionArg) {
  return new QCol<boolean>({
    defer(q, context) {
      return `IS_NULL(${q.colRef(target, context)})`;
    },
  })
}

export function if_null<T>(target: ExpressionArg<unknown>, value: ExpressionArg<T>) {
  return operation<T>((q, ctx) => `IFNULL(${q.colRef(target, ctx)}, ${q.colRef(value, ctx)})`);
}

export function and(...args: QCol<boolean>[]) {
  return operation<boolean>((q, ctx) => `(${args.map(op => q.colRef(op, ctx)).join(' AND ')})`);
}

export function or(...args: QCol<boolean>[]) {
  return operation<boolean>((q, ctx) => `(${args.map(op => q.colRef(op, ctx)).join(' OR ')})`);
}

export function xor(...args: QCol<boolean>[]) {
  return new QCol<boolean>({
    defer(q, context) {
      return args.reduce((out, col) => {
        const ref = q.colRef(col, context);
        return out ? ` XOR ${ref}` : ref;
      }, '')
    },
  })
}

export function between<T>(target: ExpressionArg<T>, start: ExpressionArg<T>, end: ExpressionArg<T>) {
  return new QCol<boolean>({
    defer(q, context) {
      return `${q.colRef(target, context)} BETWEEN ${q.colRef(start, context)} AND ${q.colRef(end, context)}`;
    }
  });
}

export function not_between<T>(target: ExpressionArg<T>, start: ExpressionArg<T>, end: ExpressionArg<T>) {
  return new QCol<boolean>({
    defer(q, context) {
      return `NOT (${q.colRef(target, context)} BETWEEN ${q.colRef(start, context)} AND ${q.colRef(end, context)})`;
    }
  });
}

export function coalesce(...args: ExpressionArg<any>[]) {
  return new QCol<any>({
    defer(q, context) {
      return `COALESCE(${args.reduce((out, arg) => {
        const ref = q.colRef(arg, context);
        return out ? `${out}, ${ref}` : ref;
      }, '')})`;
    }
  });
}

export function greatest<T extends string | number>(...args: ExpressionArg<T>[]) {
  return new QCol<T>({
    defer(q, context) {
      return `GREATEST(${args.reduce((out, arg) => {
        const ref = q.colRef(arg, context);
        return out ? `${out}, ${ref}` : ref;
      }, '')})`;
    }
  });
}

export function least<T extends string | number>(...args: ExpressionArg<T>[]) {
  return new QCol<T>({
    defer(q, context) {
      return `LEAST(${args.reduce((out, arg) => {
        const ref = q.colRef(arg, context);
        return out ? `${out}, ${ref}` : ref;
      }, '')})`;
    }
  });
}

export function iin<T>(target: ExpressionArg<T>, values: ExpressionArg<T>[]) {
  return new QCol<T>({
    defer(q, context) {
      return `${q.colRef(target, context)} IN (${values.reduce((out, arg) => {
        const ref = q.colRef(arg, context);
        return out ? `${out}, ${ref}` : ref;
      }, '')})`;
    }
  });
}

export function not_in<T>(target: ExpressionArg<T>, values: ExpressionArg<T>[]) {
  return new QCol<T>({
    defer(q, context) {
      return `${q.colRef(target, context)} NOT IN (${values.reduce((out, arg) => {
        const ref = q.colRef(arg, context);
        return out ? `${out}, ${ref}` : ref;
      }, '')})`;
    }
  });
}

export function interval(...values: ExpressionArg<number>[]) {
  return new QCol<number>({
    defer(q, context) {
      return `INTERVAL(${values.reduce((out, arg) => {
        const ref = q.colRef(arg, context);
        return out ? `${out}, ${ref}` : ref;
      }, '')})`;
    }
  });
}

// #region FLOW CONTROL FUNCTIONS

export type CaseArg<T, U> = { when: ExpressionArg<T>; then: ExpressionArg<U>; else?: ExpressionArg<U>; }
export function ccase<T, U>(target: ExpressionArg<T>, ...args: CaseArg<T, U>[]) {
  return new QCol<U>({
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

export function iif<T, U>(target: ExpressionArg<boolean>, then: ExpressionArg<T>, _else: ExpressionArg<U>) {
  return new QCol<T | U>({
    defer(q, context) {
      return `IF(${q.colRef(target, context)}, ${q.colRef(then, context)}, ${q.colRef(_else, context)})`;
    }
  });
}

export function null_if<T>(expr1: ExpressionArg<T>, expr2: ExpressionArg<T>) {
  return new QCol<T | null>({
    defer(q, context) {
      return `NULLIF(${q.colRef(expr1, context)}, ${q.colRef(expr2, context)})`;
    }
  });
}

// #endregion

// #region BIT OPERATIONS

export function bitwise_and(...args: ExpressionArg[]) {
  return new QCol({
    defer: (q, context) => args.reduce<string>((prev, arg) => {
      const ref = q.colRef(arg, context);
      return prev ? `${prev} & ${ref}` : ref;
    }, '')
  });
}

export function bitwise_shift_right(...args: ExpressionArg[]) {
  return new QCol({
    defer: (q, context) => args.reduce<string>((prev, arg) => {
      const ref = q.colRef(arg, context);
      return prev ? `${prev} >> ${ref}` : ref;
    }, '')
  });
}

export function bitwise_shift_left(...args: ExpressionArg[]) {
  return new QCol({
    defer: (q, context) => args.reduce<string>((prev, arg) => {
      const ref = q.colRef(arg, context);
      return prev ? `${prev} << ${ref}` : ref;
    }, '')
  });
}

export function bitwise_or(...args: ExpressionArg[]) {
  return new QCol({
    defer: (q, context) => args.reduce<string>((prev, arg) => {
      const ref = q.colRef(arg, context);
      return prev ? `${prev} | ${ref}` : ref;
    }, '')
  });
}

export function bitwise_xor(...args: ExpressionArg[]) {
  return new QCol({
    defer: (q, context) => args.reduce<string>((prev, arg) => {
      const ref = q.colRef(arg, context);
      return prev ? `${prev} ^ ${ref}` : ref;
    }, '')
  });
}

export function bitwise_invert(...args: ExpressionArg[]) {
  return new QCol({
    defer: (q, context) => args.reduce<string>((prev, arg) => {
      const ref = q.colRef(arg, context);
      return prev ? `${prev} ~ ${ref}` : ref;
    }, '')
  });
}

export function bit_count(arg: ExpressionArg<string | number>) {
  return new QCol({
    defer: (q, context) => `BIT_COUNT(${q.colRef(arg, context)})`
  });
}

//#endregion

// #region TYPE CONVERSIONS

export type DataType = 'BINARY' | 'CHAR' | 'DATE' | 'DATETIME' | 'DECIMAL' | 'DOUBLE' | 'FLOAT' | 'JSON' | 'NCHAR' | 'REAL' | 'SIGNED' | 'TIME' | 'UNSIGNED' | 'YEAR';

export function cast(arg: ExpressionArg, opt: { as: DataType }) {
  return new QCol({
    defer: (q, context) => `CAST(${q.colRef(arg, context)} AS ${opt.as})`
  });
}

export function convert(arg: ExpressionArg, opt: { using: string } | DataType) {
  return new QCol({
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

export function match(cols: QCol<string>[], opts: { against: ExpressionArg<string>; in?: MatchSearchModifiers & string }) {
  return new QCol<number>({
    defer: (q, ctx) => {

      let against = q.colRef(opts.against, ctx);

      if (opts.in)
        against += ` ${opts.in}`;

      return `MATCH (${cols.map(col => q.colRef(col, ctx)).join(', ')}) AGAINST (${against})`;
    }
  });
}


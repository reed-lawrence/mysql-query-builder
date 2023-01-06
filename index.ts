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
export type date = string | Date;
export type datetime = string | Date;
export type time_stamp = string;
export type time = string;

export type temporal = date | datetime | time_stamp;

const TEMPORAL_INTERVALS_NUMERIC = [
  'microsecond',
  'second',
  'minute',
  'hour',
  'day',
  'week',
  'month',
  'quarter',
  'year'
] as const;

type TemporalIntervalsNumeric = typeof TEMPORAL_INTERVALS_NUMERIC[number] | `${typeof TEMPORAL_INTERVALS_NUMERIC[number]}s`;

type TemporalIntervalsNumericValues =
  { microseconds: number } |
  { second: number } |
  { seconds: number } |
  { hour: number } |
  { hours: number } |
  { day: number } |
  { days: number } |
  { hour: number } |
  { hours: number } |
  { day: number } |
  { days: number } |
  { month: number } |
  { months: number } |
  { quarter: number } |
  { quarters: number } |
  { year: number } |
  { years: number }

const TEMPORAL_INTERVALS_STRING = [
  'second_microsecond',
  'minute_microsecond',
  'minute_second',
  'hour_microsecond',
  'hour_second',
  'hour_minute',
  'day_microsecond',
  'day_second',
  'day_minute',
  'day_hour',
  'year_month'
] as const;

type TemporalIntervalsString = typeof TEMPORAL_INTERVALS_STRING[number] | `${typeof TEMPORAL_INTERVALS_STRING[number]}s`;
type TemporalIntervalsStringValues = {
  /**
  * Format: 
  * ```typescript 
  * 'SECONDS.MICROSECONDS'
  * ```
  */
  second_microsecond: string;
} | {
  /**
   * Format: 
   * ```typescript 
   * 'SECONDS.MICROSECONDS'
   * ```
   */
  second_microseconds: string;
} | {
  /**
   * Format: 
   * ```typescript 
   * 'MINUTES:SECONDS.MICROSECONDS'
   * ```
   */
  minute_microsecond: string;
} | {
  /**
   * Format: 
   * ```typescript 
   * 'MINUTES:SECONDS.MICROSECONDS'
   * ```
   */
  minute_microseconds: string;
} | {
  /**
     * Format: 
     * ```typescript 
     * 'MINUTES:SECONDS'
     * ```
     */
  minute_second: string;
} | {
  /**
     * Format: 
     * ```typescript 
     * 'MINUTES:SECONDS'
     * ```
     */
  minute_seconds: string;
} | {
  /**
     * Format: 
     * ```typescript 
     * 'HOURS:MINUTES:SECONDS.MICROSECONDS'
     * ```
     */
  hour_microsecond: string;
} | {
  /**
     * Format: 
     * ```typescript 
     * 'HOURS:MINUTES:SECONDS.MICROSECONDS'
     * ```
     */
  hour_microseconds: string;
} | {
  /**
     * Format: 
     * ```typescript 
     * 'HOURS:MINUTES:SECONDS'
     * ```
     */
  hour_second: string;
} | {
  /**
     * Format: 
     * ```typescript 
     * 'HOURS:MINUTES:SECONDS'
     * ```
     */
  hour_seconds: string;
} | {
  /**
     * Format: 
     * ```typescript 
     * 'HOURS:MINUTES'
     * ```
     */
  hour_minute: string;
} | {
  /**
     * Format: 
     * ```typescript 
     * 'HOURS:MINUTES'
     * ```
     */
  hour_minutes: string;
} | {
  /**
     * Format: 
     * ```typescript 
     * 'DAYS HOURS:MINUTES:SECONDS.MICROSECONDS'
     * ```
     */
  day_microsecond: string;
} | {
  /**
     * Format: 
     * ```typescript 
     * 'DAYS HOURS:MINUTES:SECONDS.MICROSECONDS'
     * ```
     */
  day_microseconds: string;
} | {
  /**
   * Format: 
   * ```typescript 
   * 'DAYS HOURS:MINUTES:SECONDS'
   * ```
   */
  day_second: string;
} | {
  /**
     * Format: 
     * ```typescript 
     * 'DAYS HOURS:MINUTES:SECONDS'
     * ```
     */
  day_seconds: string;
} | {
  /**
     * Format: 
     * ```typescript 
     * 'DAYS HOURS:MINUTES'
     * ```
     */
  day_minute: string;
} | {
  /**
     * Format: 
     * ```typescript 
     * 'DAYS HOURS:MINUTES'
     * ```
     */
  day_minutes: string;
} | {
  /**
     * Format: 
     * ```typescript 
     * 'DAYS HOURS'
     * ```
     */
  day_hour: string;
} | {
  /**
     * Format: 
     * ```typescript 
     * 'DAYS HOURS'
     * ```
     */
  day_hours: string;
} | {
  /**
     * Format: 
     * ```typescript 
     * 'YEARS-MONTHS'
     * ```
     */
  year_month: string;
} | {
  /**
     * Format: 
     * ```typescript 
     * 'YEARS-MONTHS'
     * ```
     */
  year_months: string;
}

const TEMPORAL_INTERVALS = [
  ...TEMPORAL_INTERVALS_NUMERIC,
  ...TEMPORAL_INTERVALS_STRING
] as const;

type TemporalInterval = TemporalIntervalsNumeric | TemporalIntervalsString;
type TemporalIntervalObject = TemporalIntervalsNumericValues | TemporalIntervalsStringValues;
type ArgMap<T> = { [Key in keyof T]: Arg<T[Key]> }

const TEMPORAL_INTERVAL_MAP = TEMPORAL_INTERVALS.reduce((map, str) => {

  // day -> DAY
  const interval = str.toUpperCase();

  // day = DAY
  map.set(str, interval);

  // days = DAY
  map.set(`${str}s`, interval);

  return map;

}, new Map<string, string>());

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

  toCol<T extends any>(obj: Arg<T>) {
    if (obj instanceof Col)
      return obj;
    else if (obj instanceof Date)
      return new Col<string>({ path: obj.toISOString() });
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

/**
 * Returns the arc tangent of `X`, that is, the value whose tangent is `X`. 
 * Returns `NULL` if `X` is `NULL`
 * 
 * ```SQL
 * mysql> SELECT ATAN(2);
 *         -> 1.1071487177941
 * mysql> SELECT ATAN(-2);
 *         -> -1.1071487177941
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_atan
 * 
 * See also: 
 * - {@link atan2}
 */
export function atan(x: Arg<number>, y?: Arg<number>) {
  if (y)
    return new Col<number>({ defer: (q, ctx) => `ATAN(${q.colRef(x, ctx)}, ${q.colRef(y, ctx)})` });
  else
    return new Col<number>({ defer: (q, ctx) => `ATAN(${q.colRef(x, ctx)})` });
}

/**
 * Returns the arc tangent of the two variables `X` and `Y`. It is similar to calculating the arc 
 * tangent of `Y / X`, except that the signs of both arguments are used to determine the quadrant 
 * of the result. Returns `NULL` if `X` or `Y` is `NULL`.
 * 
 * ```SQL
 * mysql> SELECT ATAN(-2,2);
 *         -> -0.78539816339745
 * mysql> SELECT ATAN2(PI(),0);
 *         -> 1.5707963267949
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_atan2
 * 
 * See also: 
 * - {@link atan}
 */
export function atan2(x: Arg<number>, y: Arg<number>) {
  return new Col<number>({ defer: (q, ctx) => `ATAN2(${q.colRef(x, ctx)}, ${q.colRef(y, ctx)})` });
}

/**
 * Returns the smallest integer value not less than X. Returns NULL if X is NULL.
 * 
 * ```SQL
 * mysql> SELECT CEILING(1.23);
 *         -> 2
 * mysql> SELECT CEILING(-1.23);
 *         -> -1
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_ceiling
 */
export function ceiling(x: Arg<number>) {
  return new Col<number>({ defer: (q, ctx) => `CEILING(${q.colRef(x, ctx)})` });
}

/**
 * `CEIL()` is a synonym for `CEILING()`.
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_ceil
 * 
 * See also: 
 * - {@link ceiling}
 */
export const ciel = ceiling;

/**
 * Converts numbers between different number bases. Returns a string representation of the number `N`, 
 * converted from base `from_base` to base `to_base`. Returns `NULL` if any argument is `NULL`. The 
 * argument `N` is interpreted as an integer, but may be specified as an integer or a string. The 
 * minimum base is `2` and the maximum base is `36`. If `from_base` is a negative number, `N` is 
 * regarded as a signed number. Otherwise, `N` is treated as unsigned. `CONV()` works with 64-bit precision.
 * 
 * `CONV()` returns `NULL` if any of its arguments are `NULL`.
 * 
 * ```SQL
 * mysql> SELECT CONV('a',16,2);
 *         -> '1010'
 * mysql> SELECT CONV('6E',18,8);
 *         -> '172'
 * mysql> SELECT CONV(-17,10,-18);
 *         -> '-H'
 * mysql> SELECT CONV(10+'10'+'10'+X'0a',10,10);
 *         -> '40'
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_conv
 */
export function conv(n: Arg<number | string>, from_base: Arg<number>, to_base: Arg<number>) {
  return new Col<number>({ defer: (q, ctx) => `CONV(${q.colRef(n, ctx)}, ${q.colRef(from_base, ctx)}, ${q.colRef(to_base, ctx)})` });
}


/**
 * Returns the cosine of `X`. Returns `NULL` if `X` is `NULL`.
 * 
 * ```SQL
 * mysql> SELECT COT(12);
 *         -> -1.5726734063977
 * mysql> SELECT COT(0);
 *         -- out-of-range error
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_cos
 */
export function cos(x: Arg<number>) {
  return new Col<number>({ defer: (q, ctx) => `COS(${q.colRef(x, ctx)})` });
}

/**
 * Returns the cotangent of `X`. Returns `NULL` if `X` is `NULL`.
 * 
 * ```SQL
 * mysql> SELECT COT(12);
 *         -> -1.5726734063977
 * mysql> SELECT COT(0);
 *         -> out-of-range error
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_cot
 */
export function cot(x: Arg<number>) {
  return new Col<number>({ defer: (q, ctx) => `COT(${q.colRef(x, ctx)})` });
}

/**
 * Computes a cyclic redundancy check value and returns a 32-bit unsigned value. The result 
 * is `NULL` if the argument is `NULL`. The argument is expected to be a string and (if 
 * possible) is treated as one if it is not.
 * 
 * ```SQL
 * mysql> SELECT CRC32('MySQL');
 *         -> 3259397556
 * mysql> SELECT CRC32('mysql');
 *         -> 2501908538
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_crc32
 */
export function crc32(expr: Arg<string>) {
  return new Col<number>({ defer: (q, ctx) => `CRC32(${q.colRef(expr, ctx)})` });
}

/**
 * Returns the argument `X`, converted from radians to degrees. Returns `NULL` if `X` is `NULL`.
 * 
 * ```SQL
 * mysql> SELECT DEGREES(PI());
 *         -> 180
 * mysql> SELECT DEGREES(PI() / 2);
 *         -> 90
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_degrees
 * 
 * See also: 
 * - {@link radians}
 */
export function degrees(x: Arg<number>) {
  return new Col<number>({ defer: (q, ctx) => `DEGREES(${q.colRef(x, ctx)})` });
}

/**
 * Returns the value of e (the base of natural logarithms) raised to the power of `X`. The 
 * inverse of this function is `LOG()` (using a single argument only) or `LN()`.
 * 
 * If `X` is `NULL`, this function returns `NULL`.
 * 
 * ```SQL
 * mysql> SELECT EXP(2);
 *         -> 7.3890560989307
 * mysql> SELECT EXP(-2);
 *         -> 0.13533528323661
 * mysql> SELECT EXP(0);
 *         -> 1
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_exp
 */
export function exp(x: Arg<number>) {
  return new Col<number>({ defer: (q, ctx) => `EXP(${q.colRef(x, ctx)})` });
}

/**
 * Returns the largest integer value not greater than `X`. Returns `NULL` if `X` is `NULL`.
 * 
 * ```SQL
 * mysql> SELECT FLOOR(1.23), FLOOR(-1.23);
 *         -> 1, -2
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_floor
 */
export function floor(x: Arg<number>) {
  return new Col<number>({ defer: (q, ctx) => `FLOOR(${q.colRef(x, ctx)})` });
}

/**
 * Returns the natural logarithm of `X`; that is, the base-e logarithm of `X`. If `X` is less 
 * than or equal to `0.0E0`, the function returns `NULL` and a warning “Invalid argument for logarithm” 
 * is reported. 
 * 
 * Returns `NULL` if X is `NULL`.
 * 
 * ```SQL
 * mysql> SELECT LN(2);
 *         -> 0.69314718055995
 * mysql> SELECT LN(-2);
 *         -> NULL
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_ln
 */
export function ln(x: Arg<number>) {
  return new Col<number>({ defer: (q, ctx) => `LN(${q.colRef(x, ctx)})` });
}

/**
 * If called with two parameters, this function returns the logarithm of `X` to the base `B`. 
 * If `X` is less than or equal to `0`, or if `B` is less than or equal to `1`, then NULL is returned.
 * 
 * Returns `NULL` if `X` or `B` are `NULL`.
 * 
 * `LOG(B,X)` is equivalent to `LOG(X) / LOG(B)`.
 * 
 *  * ```SQL
 * mysql> SELECT LOG(2,65536);
 *         -> 16
 * mysql> SELECT LOG(10,100);
 *         -> 2
 * mysql> SELECT LOG(1,100);
 *         -> NULL
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_log
 */
export function log(x: Arg<number>, b: Arg<number>): Col<number>;

/**
 * If called with one parameter, this function returns the natural logarithm of `X`. If `X`
 * is less than or equal to `0.0E0`, the function returns `NULL` and a warning “Invalid argument
 * for logarithm” is reported. 
 * 
 * Returns `NULL` if `X` is `NULL`.
 * 
 * The inverse of this function (when called with a single argument) is the `EXP()` function.
 * 
 * ```SQL
 * mysql> SELECT LOG(2);
 *         -> 0.69314718055995
 * mysql> SELECT LOG(-2);
 *         -> NULL
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_log
 */
export function log(x: Arg<number>): Col<number>;
export function log(x: Arg<number>, b?: Arg<number>) {
  if (b !== undefined)
    return new Col<number>({ defer: (q, ctx) => `LOG(${q.colRef(x, ctx)}, ${q.colRef(b, ctx)})` });
  else
    return new Col<number>({ defer: (q, ctx) => `LOG(${q.colRef(x, ctx)})` });
}


/**
 * Returns the base-2 logarithm of `X`. If `X` is less than or equal to `0.0E0`, the function 
 * returns `NULL` and a warning “Invalid argument for logarithm” is reported. Returns `NULL` 
 * if `X` is `NULL`.
 * 
 * ```SQL
 * mysql> SELECT LOG2(65536);
 *         -> 16
 * mysql> SELECT LOG2(-100);
 *         -> NULL
 * ```
 * 
 * `LOG2()` is useful for finding out how many bits a number requires for storage. This 
 * function is equivalent to the expression `LOG(X) / LOG(2)`.
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_log2
 * 
 * See also: 
 * - {@link log}
 */
export function log2(x: Arg<number>) {
  return new Col<number>({ defer: (q, ctx) => `LOG2(${q.colRef(x, ctx)})` });
}

/**
 * Returns the base-10 logarithm of `X`. If `X` is less than or equal to `0.0E0`, the function 
 * returns `NULL` and a warning “Invalid argument for logarithm” is reported. Returns `NULL` 
 * if `X` is `NULL`.
 * 
 * ```SQL
 * mysql> SELECT LOG10(2);
 *         -> 0.30102999566398
 * mysql> SELECT LOG10(100);
 *         -> 2
 * mysql> SELECT LOG10(-100);
 *         -> NULL
 * ```
 * 
 * `LOG10(X)` is equivalent to `LOG(10,X)`.
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_log10
 * 
 * See also: 
 * - {@link log}
 */
export function log10(x: Arg<number>) {
  return new Col<number>({ defer: (q, ctx) => `LOG10(${q.colRef(x, ctx)})` });
}

/**
 * Modulo operation. Returns the remainder of `N` divided by `M`. Returns 
 * `NULL` if `M` or `N` is NULL.
 * 
 * ```SQL
 * mysql> SELECT MOD(234, 10);
 *         -> 4
 * mysql> SELECT 253 % 7;
 *         -> 1
 * mysql> SELECT MOD(29, 9);
 *         -> 2
 * mysql> SELECT 29 MOD 9;
 *         -> 2
 * ```
 * 
 * This function is safe to use with `BIGINT` values.
 * 
 * `MOD()` also works on values that have a fractional part and returns the exact remainder 
 * after division:
 * 
 * ```SQL
 * mysql> SELECT MOD(34.5,3);
 *         -> 1.5
 * ```
 * 
 * `MOD(N,0)` returns `NULL`.
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_mod
 * 
 * See also: 
 * - {@link modulo}
 */
export function mod(m: Arg<number>, n: Arg<number>) {
  return new Col<number>({ defer: (q, ctx) => `MOD(${q.colRef(m, ctx)}, ${q.colRef(n, ctx)})` });
}

/**
 * eturns the value of π (pi). The default number of decimal places displayed is seven, 
 * but MySQL uses the full double-precision value internally.
 * 
 * ```SQL
 * mysql> SELECT PI();
 *         -> 3.141593
 * mysql> SELECT PI() + 0.000000000000000000;
 *         -> 3.141592653589793116
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_pi
 */
export function pi() {
  return new Col<number>({ defer: () => `PI()` });
}

/**
 * Returns the value of `X` raised to the power of `Y`. Returns `NULL` if `X` or `Y` is `NULL`.
 * 
 * ```SQL
 * mysql> SELECT POW(2,2);
 *         -> 4
 * mysql> SELECT POW(2,-2);
 *         -> 0.25
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_pow
 */
export function pow(x: Arg<number>, y: Arg<number>) {
  return new Col<number>({ defer: (q, ctx) => `POW(${q.colRef(x, ctx)}, ${q.colRef(y, ctx)})` });
}

/**
 * A synonym for `POW()`.
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_power
 * 
 * See also: 
 * - {@link pow}
 */
export const power = pow;


/**
 * Returns the argument `X`, converted from degrees to radians. (Note that π radians equals 180 degrees.) 
 * 
 * Returns `NULL` if `X` is `NULL`.
 * 
 * ```SQL
 * mysql> SELECT RADIANS(90);
 *         -> 1.5707963267949
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_radians
 * 
 * See also: 
 * - {@link degrees}
 */
export function radians(x: Arg<number>) {
  return new Col<number>({ defer: (q, ctx) => `RADIANS(${q.colRef(x, ctx)})` });
}

/**
 * Returns a random floating-point value `v` in the range `0 <= v < 1.0`. To obtain a random 
 * integer `R` in the range `i <= R < j`, use the expression `FLOOR(i + RAND() * (j − i))`. 
 * 
 * For example, to obtain a random integer in the range the range `7 <= R < 12`, use the 
 * following statement:
 * 
 * ```SQL
 * SELECT FLOOR(7 + (RAND() * 5));
 * ```
 * 
 * If an integer argument `N` is specified, it is used as the seed value:
 * - With a constant initializer argument, the seed is initialized once when the statement 
 * is prepared, prior to execution.
 * - With a nonconstant initializer argument (such as a column name), the seed is initialized with 
 * the value for each invocation of `RAND()`.
 * 
 * One implication of this behavior is that for equal argument values, `RAND(N)` returns the same 
 * value each time, and thus produces a repeatable sequence of column values. In the following 
 * example, the sequence of values produced by `RAND(3)` is the same both places it occurs.
 * 
 * ```SQL
 * mysql> CREATE TABLE t (i INT);
 * 
 * mysql> INSERT INTO t VALUES(1),(2),(3);
 * 
 * mysql> SELECT i, RAND() FROM t;
 * --     +------+------------------+
 * --     | i    | RAND()           |
 * --     +------+------------------+
 * --     |    1 | 0.61914388706828 |
 * --     |    2 | 0.93845168309142 |
 * --     |    3 | 0.83482678498591 |
 * --     +------+------------------+
 * 
 * mysql> SELECT i, RAND(3) FROM t;
 * --     +------+------------------+
 * --     | i    | RAND(3)          |
 * --     +------+------------------+
 * --     |    1 | 0.90576975597606 |
 * --     |    2 | 0.37307905813035 |
 * --     |    3 | 0.14808605345719 |
 * --     +------+------------------+
 * 
 * mysql> SELECT i, RAND() FROM t;
 * --     +------+------------------+
 * --     | i    | RAND()           |
 * --     +------+------------------+
 * --     |    1 | 0.35877890638893 |
 * --     |    2 | 0.28941420772058 |
 * --     |    3 | 0.37073435016976 |
 * --     +------+------------------+
 * 
 * mysql> SELECT i, RAND(3) FROM t;
 * --     +------+------------------+
 * --     | i    | RAND(3)          |
 * --     +------+------------------+
 * --     |    1 | 0.90576975597606 |
 * --     |    2 | 0.37307905813035 |
 * --     |    3 | 0.14808605345719 |
 * --     +------+------------------+
 * ```
 * 
 * `RAND()` in a `WHERE` clause is evaluated for every row (when selecting from one table) 
 * or combination of rows (when selecting from a multiple-table join). Thus, for optimizer 
 * purposes, `RAND()` is not a constant value and cannot be used for index optimizations. 
 * For more information, see 
 * [Section 8.2.1.20, “Function Call Optimization”](https://dev.mysql.com/doc/refman/8.0/en/function-optimization.html).
 * 
 * Use of a column with `RAND()` values in an `ORDER BY` or `GROUP BY` clause may yield 
 * unexpected results because for either clause a `RAND()` expression can be evaluated 
 * multiple times for the same row, each time returning a different result. If the goal 
 * is to retrieve rows in random order, you can use a statement like this:
 * ```SQL
 * SELECT * FROM tbl_name ORDER BY RAND();
 * ```
 * 
 * To select a random sample from a set of rows, combine ORDER BY RAND() with LIMIT:
 * ```SQL
 * SELECT * FROM table1, table2 WHERE a=b AND c<d ORDER BY RAND() LIMIT 1000;
 * ```
 * `RAND()` is not meant to be a perfect random generator. It is a fast way to generate 
 * random numbers on demand that is portable between platforms for the same MySQL version.
 * 
 * This function is unsafe for statement-based replication. A warning is logged if you 
 * use this function when 
 * [`binlog_format`](https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_binlog_format)
 * is set to `STATEMENT`.
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/mathematical-functions.html#function_rand
 */
export function rand(seed?: Arg<number>) {
  if (seed !== undefined)
    return new Col<number>({ defer: (q, ctx) => `RAND(${q.colRef(seed, ctx)})` });
  else
    return new Col<number>({ defer: () => `RAND()` });
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

/**
 * When invoked with the `INTERVAL` form of the second argument, `ADDDATE()` is a synonym for `DATE_ADD()`. 
 * The related function `SUBDATE()` is a synonym for `DATE_SUB()`. For information on the `INTERVAL` unit 
 * argument, see 
 * [Temporal Intervals](https://dev.mysql.com/doc/refman/8.0/en/expressions.html#temporal-intervals).
 * 
 * ```SQL
 * mysql> SELECT DATE_ADD('2008-01-02', INTERVAL 31 DAY);
 *         -> '2008-02-02'
 * mysql> SELECT ADDDATE('2008-01-02', INTERVAL 31 DAY);
 *         -> '2008-02-02'
 * ```
 * 
 * When invoked with the days form of the second argument, MySQL treats it as an integer number of days to be added to expr.
 * 
 * ```SQL
 * mysql> SELECT ADDDATE('2008-01-02', 31);
 *         -> '2008-02-02'
 * ```
 * 
 * This function returns `NULL` if `date` or `days` is `NULL`.
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_adddate
 */
export function adddate(date: Arg<date>, arg: Arg<number> | ArgMap<TemporalIntervalObject>): Col<date> {

  if (typeof arg === 'number' || arg instanceof Col)
    return new Col<date>({
      defer(q, context) {
        return `ADDDATE(${q.colRef(date, context)}, ${q.colRef(arg, context)})`;
      }
    });

  else
    return new Col<date>({
      defer(q, context) {
        return Object.entries(arg).reduceRight((out, [key, value]) => {

          const interval = TEMPORAL_INTERVAL_MAP.get(key) ?? (() => { throw new Error(`Key ${key} is does not match any TemporalIntervalObject key`) })();

          return `ADDDATE(${out}, INTERVAL ${q.colRef(value, context)} ${interval})`;

        }, q.colRef(date, context));
      }
    });

}

/**
 * These functions perform date arithmetic. The `date` argument specifies the starting `date` or `datetime` value. 
 * `expr` is an expression specifying the interval value to be added or subtracted from the starting `date`. `expr` 
 * is evaluated as a `string`; it may start with a `-` for negative intervals. `unit` is a keyword indicating the 
 * units in which the expression should be interpreted.
 * 
 * For more information about temporal interval syntax, including a full list of unit specifiers, the expected form 
 * of the `expr` argument for each unit value, and rules for operand interpretation in temporal arithmetic, see 
 * [Temporal Intervals](https://dev.mysql.com/doc/refman/8.0/en/expressions.html#temporal-intervals).
 * 
 * #### Usage
 * ```typescript
 * date_add('2023-01-01', { days: 10 }); // => Col<date> -> DATE_ADD('2023-01-01', INTERVAL 10 DAY);
 * 
 * date_add('2023-01-01', { months: 3, year: 1 }); // => Col<date> -> DATE_ADD(DATE_ADD('2023-01-01', INTERVAL 3 MONTH), INTERVAL 1 YEAR);
 * 
 * ```
 * 
 * The return value depends on the arguments:
 * - If date is `NULL`, the function returns `NULL`.
 * - `DATE` if the date argument is a `DATE` value and your calculations involve only `YEAR`, `MONTH`, and `DAY` 
 *   parts (that is, no time parts).
 * - (MySQL 8.0.28 and later:) `TIME` if the date argument is a `TIME` value and the calculations involve only `HOURS`,
 *   `MINUTES`, and `SECONDS` parts (that is, no date parts).
 * - `DATETIME` if the first argument is a `DATETIME` (or `TIMESTAMP`) value, or if the first argument is a `DATE` 
 *   and the unit value uses `HOURS`, `MINUTES`, or `SECONDS`, or if the first argument is of type `TIME` and the 
 *   unit value uses `YEAR`, `MONTH`, or `DAY`.
 * - (MySQL 8.0.28 and later:) If the first argument is a dynamic parameter (for example, of a prepared statement), 
 *   its resolved type is `DATE` if the second argument is an interval that contains some combination of `YEAR`, `MONTH`, 
 *   or `DAY` values only; otherwise, its type is `DATETIME`.
 * - String otherwise (type `VARCHAR`).
 * 
 * > #### Note
 * > In MySQL 8.0.22 through 8.0.27, when used in prepared statements, these functions returned `DATETIME` 
 * > values regardless of argument types. (Bug #103781)
 * 
 * To ensure that the result is `DATETIME`, you can use `CAST()` to convert the first argument to `DATETIME`.
 * 
 * ```SQL
 * mysql> SELECT DATE_ADD('2018-05-01',INTERVAL 1 DAY);
 *         -> '2018-05-02'
 * mysql> SELECT DATE_SUB('2018-05-01',INTERVAL 1 YEAR);
 *         -> '2017-05-01'
 * mysql> SELECT DATE_ADD('2020-12-31 23:59:59', INTERVAL 1 SECOND);
 *         -> '2021-01-01 00:00:00'
 * mysql> SELECT DATE_ADD('2018-12-31 23:59:59', INTERVAL 1 DAY);
 *         -> '2019-01-01 23:59:59'
 * mysql> SELECT DATE_ADD('2100-12-31 23:59:59', INTERVAL '1:1' MINUTE_SECOND);
 *         -> '2101-01-01 00:01:00'
 * mysql> SELECT DATE_SUB('2025-01-01 00:00:00', INTERVAL '1 1:1:1' DAY_SECOND);
 *         -> '2024-12-30 22:58:59'
 * mysql> SELECT DATE_ADD('1900-01-01 00:00:00', INTERVAL '-1 10' DAY_HOUR);
 *         -> '1899-12-30 14:00:00'
 * mysql> SELECT DATE_SUB('1998-01-02', INTERVAL 31 DAY);
 *         -> '1997-12-02'
 * mysql> SELECT DATE_ADD('1992-12-31 23:59:59.000002', INTERVAL '1.999999' SECOND_MICROSECOND);
 *         -> '1993-01-01 00:00:01.000001'
 * ```
 * 
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-add
 * 
 * See also:
 * - {@link cast}
 * - {@link date_sub}
 * - {@link TemporalInterval}
 * - {@link TEMPORAL_INTERVALS}
 */
export function date_add(date: Arg<date>, arg: ArgMap<TemporalIntervalObject>): Col<date> {
  return new Col<date>({
    defer(q, context) {
      return Object.entries(arg).reduceRight((out, [key, value]) => {

        const interval = TEMPORAL_INTERVAL_MAP.get(key) ?? (() => { throw new Error(`Key ${key} is does not match any TemporalIntervalObject key`) })();

        return `DATE_ADD(${out}, INTERVAL ${q.colRef(value, context)} ${interval})`;

      }, q.colRef(date, context));
    }
  });
}

/**
 * Formats the `date` value according to the `format` string. If either argument is `NULL`, 
 * the function returns `NULL`.
 * 
 * The specifiers shown in the following table may be used in the format string. The `%` character 
 * is required before format specifier characters. The specifiers apply to other functions 
 * as well: `STR_TO_DATE()`, `TIME_FORMAT()`, `UNIX_TIMESTAMP()`.
 * 
 * ```SQL
 * mysql> SELECT DATE_FORMAT('2009-10-04 22:23:00', '%W %M %Y');
 *         -> 'Sunday October 2009'
 * mysql> SELECT DATE_FORMAT('2007-10-04 22:23:00', '%H:%i:%s');
 *         -> '22:23:00'
 * mysql> SELECT DATE_FORMAT('1900-10-04 22:23:00', '%D %y %a %d %m %b %j');
 *         -> '4th 00 Thu 04 10 Oct 277'
 * mysql> SELECT DATE_FORMAT('1997-10-04 22:23:00', '%H %k %I %r %T %S %w');
 *         -> '22 22 10 10:23:00 PM 22:23:00 00 6'
 * mysql> SELECT DATE_FORMAT('1999-01-01', '%X %V');
 *         -> '1998 52'
 * mysql> SELECT DATE_FORMAT('2006-06-00', '%d');
 *         -> '00'
 * ```
 * 
 * | Specifier | Description|
 * |-----------|------------|
 * | %a |	Abbreviated weekday name (Sun..Sat) |
 * | %b |	Abbreviated month name (Jan..Dec) |
 * | %c |	Month, numeric (0..12) |
 * | %D |	Day of the month with English suffix (0th, 1st, 2nd, 3rd, …) |
 * | %d |	Day of the month, numeric (00..31) |
 * | %e |	Day of the month, numeric (0..31) |
 * | %f |	Microseconds (000000..999999) |
 * | %H |	Hour (00..23) |
 * | %h |	Hour (01..12) |
 * | %I |	Hour (01..12) |
 * | %i |	Minutes, numeric (00..59) |
 * | %j |	Day of year (001..366) |
 * | %k |	Hour (0..23) |
 * | %l |	Hour (1..12) |
 * | %M |	Month name (January..December) |
 * | %m |	Month, numeric (00..12) |
 * | %p |	AM or PM |
 * | %r |	Time, 12-hour (hh:mm:ss followed by AM or PM) |
 * | %S |	Seconds (00..59) |
 * | %s |	Seconds (00..59) |
 * | %T |	Time, 24-hour (hh:mm:ss) |
 * | %U |	Week (00..53), where Sunday is the first day of the week; WEEK() mode 0 |
 * | %u |	Week (00..53), where Monday is the first day of the week; WEEK() mode 1 |
 * | %V |	Week (01..53), where Sunday is the first day of the week; WEEK() mode 2; used with %X |
 * | %v |	Week (01..53), where Monday is the first day of the week; WEEK() mode 3; used with %x |
 * | %W |	Weekday name (Sunday..Saturday) |
 * | %w |	Day of the week (0=Sunday..6=Saturday) |
 * | %X |	Year for the week where Sunday is the first day of the week, numeric, four digits; used with %V |
 * | %x |	Year for the week, where Monday is the first day of the week, numeric, four digits; used with %v |
 * | %Y |	Year, numeric, four digits |
 * | %y |	Year, numeric (two digits) |
 * | %% |	A literal % character |
 * | %x |	x, for any “x” not listed above |
 * 
 * Ranges for the month and day specifiers begin with zero due to the fact that MySQL 
 * permits the storing of incomplete dates such as `'2014-00-00'`.
 * 
 * The language used for day and month names and abbreviations is controlled by the value of 
 * the [`lc_time_names`](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_lc_time_names) 
 * system variable 
 * [(Section 10.16, “MySQL Server Locale Support”)](https://dev.mysql.com/doc/refman/8.0/en/locale-support.html).
 * 
 * For the `%U`, `%u`, `%V`, and `%v` specifiers, see the description of the `WEEK()` function for information about 
 * the mode values. The mode affects how week numbering occurs.
 * 
 * `DATE_FORMAT()` returns a string with a character set and collation given by `character_set_connection` and 
 * `collation_connection` so that it can return month and weekday names containing non-ASCII characters.
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-format
 * 
 * See also: 
 * - {@link str_to_date}
 * - {@link time_format}
 * - {@link unix_timestamp}
 * - {@link week}
 */
export function date_format(date: Arg<date>, format: Arg<string>): Col<string> {
  return new Col({
    defer(q, context) {
      return `DATE_FORMAT(${q.colRef(date, context)}, ${q.colRef(format, context)})`
    }
  });
}

/**
 * These functions perform date arithmetic. The `date` argument specifies the starting `date` or `datetime` value. 
 * `expr` is an expression specifying the interval value to be added or subtracted from the starting `date`. `expr` 
 * is evaluated as a `string`; it may start with a `-` for negative intervals. `unit` is a keyword indicating the 
 * units in which the expression should be interpreted.
 * 
 * For more information about temporal interval syntax, including a full list of unit specifiers, the expected form 
 * of the `expr` argument for each unit value, and rules for operand interpretation in temporal arithmetic, see 
 * [Temporal Intervals](https://dev.mysql.com/doc/refman/8.0/en/expressions.html#temporal-intervals).
 * 
 * #### Usage
 * ```typescript
 * date_sub('2023-01-01', { days: 10 }); // => Col<date> -> DATE_SUB('2023-01-01', INTERVAL 10 DAY);
 * 
 * date_sub('2023-01-01', { months: 3, year: 1 }); // => Col<date> -> DATE_SUB(DATE_SUB('2023-01-01', INTERVAL 3 MONTH), INTERVAL 1 YEAR);
 * 
 * ```
 * 
 * The return value depends on the arguments:
 * - If date is `NULL`, the function returns `NULL`.
 * - `DATE` if the date argument is a `DATE` value and your calculations involve only `YEAR`, `MONTH`, and `DAY` 
 *   parts (that is, no time parts).
 * - (MySQL 8.0.28 and later:) `TIME` if the date argument is a `TIME` value and the calculations involve only `HOURS`,
 *   `MINUTES`, and `SECONDS` parts (that is, no date parts).
 * - `DATETIME` if the first argument is a `DATETIME` (or `TIMESTAMP`) value, or if the first argument is a `DATE` 
 *   and the unit value uses `HOURS`, `MINUTES`, or `SECONDS`, or if the first argument is of type `TIME` and the 
 *   unit value uses `YEAR`, `MONTH`, or `DAY`.
 * - (MySQL 8.0.28 and later:) If the first argument is a dynamic parameter (for example, of a prepared statement), 
 *   its resolved type is `DATE` if the second argument is an interval that contains some combination of `YEAR`, `MONTH`, 
 *   or `DAY` values only; otherwise, its type is `DATETIME`.
 * - String otherwise (type `VARCHAR`).
 * 
 * > #### Note
 * > In MySQL 8.0.22 through 8.0.27, when used in prepared statements, these functions returned `DATETIME` 
 * > values regardless of argument types. (Bug #103781)
 * 
 * To ensure that the result is `DATETIME`, you can use `CAST()` to convert the first argument to `DATETIME`.
 * 
 * ```SQL
 * mysql> SELECT DATE_SUB('2018-05-01',INTERVAL 1 YEAR);
 *         -> '2017-05-01'
 * mysql> SELECT DATE_SUB('2025-01-01 00:00:00', INTERVAL '1 1:1:1' DAY_SECOND);
 *         -> '2024-12-30 22:58:59'
 * mysql> SELECT DATE_SUB('1998-01-02', INTERVAL 31 DAY);
 *         -> '1997-12-02'
 * ```
 * 
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-sub
 * 
 * See also:
 * - {@link cast}
 * - {@link date_add}
 * - {@link TemporalInterval}
 * - {@link TEMPORAL_INTERVALS}
 */
export function date_sub(date: Arg<date>, arg: ArgMap<TemporalIntervalObject>): Col<date> {
  return new Col({
    defer(q, context) {
      return Object.entries(arg).reduceRight((out, [key, value]) => {

        const interval = TEMPORAL_INTERVAL_MAP.get(key) ?? (() => { throw new Error(`Key ${key} is does not match any TemporalIntervalObject key`) })();

        return `DATE_SUB(${out}, INTERVAL ${q.colRef(value, context)} ${interval})`;

      }, q.colRef(date, context));
    }
  });
}

/**
 * Internally proxies {@link dayofmonth}
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_day
 */
export const day = dayofmonth;

/**
 * Returns the name of the weekday for `date`. The language used for the name is controlled 
 * by the value of the 
 * [`lc_time_names`](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_lc_time_names) 
 * system variable see
 * [Section 10.16, “MySQL Server Locale Support”](https://dev.mysql.com/doc/refman/8.0/en/locale-support.html). 
 * 
 * Returns `NULL` if date is `NULL`.
 * 
 * ```SQL
 * mysql> SELECT DAYNAME('2007-02-03');
 *         -> 'Saturday'
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_dayname
 */
export function dayname(date: Arg<date>): Col<string> {
  return new Col({
    defer(q, context) {
      return `DAYNAME(${q.colRef(date, context)})`;
    }
  });
}

/**
 * Returns the day of the month for date, in the range `1` to `31`, or `0` for dates such as 
 * `'0000-00-00'` or `'2008-00-00'` that have a zero day part. 
 * 
 * Returns `NULL` if date is `NULL`.
 * 
 * ```SQL
 * mysql> SELECT DAYOFMONTH('2007-02-03');
 *         -> 3
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_dayofmonth
 * 
 * See also: 
 * - {@link day}
 */
export function dayofmonth(date: Arg<date>): Col<number> {
  return new Col({
    defer(q, context) {
      return `DAYOFMONTH(${q.colRef(date, context)}`;
    }
  });
}

export function timestamp(date: Arg<date>): Col<time_stamp> {
  return new Col({
    defer(q, context) {
      return `TIMESTAMP(${q.colRef(date, context)})`;
    }
  });
}

/**
 * Returns the weekday index for `date` (1 = Sunday, 2 = Monday, …, 7 = Saturday). These index 
 * values correspond to the ODBC standard. Returns `NULL` if date is `NULL`.
 * 
 * ```SQL
 * mysql> SELECT DAYOFWEEK('2007-02-03');
 *         -> 7
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_dayofweek
 */
export function dayofweek(date: Arg<date>): Col<number> {
  return new Col({
    defer(q, context) {
      return `DAYOFWEEK(${q.colRef(date, context)})`;
    }
  });
}

/**
 * Returns the day of the year for `date`, in the range `1` to `366`. Returns `NULL` if date is `NULL`.
 * 
 * ```SQL
 * mysql> SELECT DAYOFYEAR('2007-02-03');
 *         -> 34
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_dayofyear
 */
export function dayofyear(date: Arg<date>): Col<number> {
  return new Col({
    defer(q, context) {
      return `DAYOFYEAR(${q.colRef(date, context)})`;
    }
  });
}

/**
 * ```typescript
 * // TypeScript
 * extract('year', { from: '2019-07-02' }) // => Col<number> -> EXTRACT(YEAR FROM '2019-07-02')
 * ```
 * #
 * The `EXTRACT()` function uses the same kinds of unit specifiers as `DATE_ADD()` or `DATE_SUB()`, but 
 * extracts parts from the date rather than performing date arithmetic. For information on the unit 
 * argument, see {@link TemporalInterval Temporal Intervals}. 
 * 
 * Returns `NULL` if date is `NULL`.
 * 
 * ```SQL
 * mysql> SELECT EXTRACT(YEAR FROM '2019-07-02');
 *         -> 2019
 * mysql> SELECT EXTRACT(YEAR_MONTH FROM '2019-07-02 01:02:03');
 *         -> 201907
 * mysql> SELECT EXTRACT(DAY_MINUTE FROM '2019-07-02 01:02:03');
 *         -> 20102
 * mysql> SELECT EXTRACT(MICROSECOND FROM '2003-01-02 10:30:00.000123');
 *         -> 123
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_extract
 */
export function extract(unit: TemporalInterval, arg: { from: Arg<date> }): Col<number> {
  return new Col({
    defer(q, context) {
      const interval = TEMPORAL_INTERVAL_MAP.get(unit) ?? (() => { throw new Error(`Key ${unit} does not match any TEMPORAL_INTERVAL_MAP key`) })();

      return `EXTRACT(${interval} FROM ${q.colRef(arg.from, context)})`;
    }
  })
}

/**
 * Given a day number `N`, returns a `DATE` value. Returns `NULL` if `N` is `NULL`.
 * 
 * ```SQL
 * mysql> SELECT FROM_DAYS(730669);
 *         -> '2000-07-03'
 * ```
 * 
 * Use `FROM_DAYS()` with caution on old dates. It is not intended for use with values that 
 * precede the advent of the Gregorian calendar (`1582`). See 
 * [Section 12.9, “What Calendar Is Used By MySQL?”](https://dev.mysql.com/doc/refman/8.0/en/mysql-calendar.html).
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_from-days
 */
export function from_days(n: Arg<number>): Col<date> {
  return new Col({
    defer(q, context) {
      return `FROM_DAYS(${q.colRef(n, context)})`;
    }
  });
}

/**
 * Returns a representation of `unix_timestamp` as a `datetime` or character `string` value. 
 * The value returned is expressed using the session time zone. (Clients can set the session 
 * time zone as described in [Section 5.1.15, “MySQL Server Time Zone Support”](https://dev.mysql.com/doc/refman/8.0/en/time-zone-support.html).) 
 * `unix_timestamp` is an internal timestamp value representing seconds since `'1970-01-01 00:00:00'` 
 * UTC, such as produced by the `UNIX_TIMESTAMP()` function.
 * 
 * If format is omitted, this function returns a `DATETIME` value.
 * 
 * If `unix_timestamp` or format is `NULL`, this function returns `NULL`.
 * 
 * If `unix_timestamp` is an `integer`, the fractional seconds precision of the `DATETIME` is 
 * zero. When `unix_timestamp` is a `decimal` value, the fractional seconds precision of the 
 * `DATETIME` is the same as the precision of the `decimal` value, up to a maximum of 6. When 
 * `unix_timestamp` is a floating point number, the fractional seconds precision of the 
 * datetime is 6.
 * 
 * On 32-bit platforms, the maximum useful value for `unix_timestamp` is `2147483647.999999`, 
 * which returns `'2038-01-19 03:14:07.999999'` UTC. On 64-bit platforms running MySQL 8.0.28 
 * or later, the effective maximum is `32536771199.999999`, which returns `'3001-01-18 23:59:59.999999'` 
 * UTC. Regardless of platform or version, a greater value for `unix_timestamp` than the effective 
 * maximum returns `0`.
 * 
 * `format` is used to format the result in the same way as the format string used for the 
 * `DATE_FORMAT()` function. If format is supplied, the value returned is a `VARCHAR`.
 * 
 * ```SQL
 * mysql> SELECT FROM_UNIXTIME(1447430881);
 *         -> '2015-11-13 10:08:01'
 * mysql> SELECT FROM_UNIXTIME(1447430881) + 0;
 *         -> 20151113100801
 * mysql> SELECT FROM_UNIXTIME(1447430881, '%Y %D %M %h:%i:%s %x');
 *         -> '2015 13th November 10:08:01 2015'
 * ```
 * 
 * #### Note
 * > If you use `UNIX_TIMESTAMP()` and `FROM_UNIXTIME()` to convert between values in a 
 * > non-UTC time zone and Unix timestamp values, the conversion is lossy because the 
 * > mapping is not one-to-one in both directions. For details, see the description of 
 * > the `UNIX_TIMESTAMP()` function.
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_from-unixtime
 * 
 * See also: 
 * - {@link unix_timestamp}
 */
export function from_unixtime(unix_timestamp: Arg<number>, format: Arg<string>): Col<string>;
export function from_unixtime(unix_timestamp: Arg<number>): Col<date>;
export function from_unixtime(unix_timestamp: Arg<number>, format?: Arg<string>): Col {
  if (format !== undefined)
    return new Col({
      defer(q, context) {
        return `FROM_UNIXTIME(${q.colRef(unix_timestamp, context)}, ${q.colRef(format, context)})`;
      },
    });
  else
    return new Col({
      defer(q, context) {
        return `FROM_UNIXTIME(${q.colRef(unix_timestamp, context)})`;
      },
    });
}

/**
 * Returns a format string. This function is useful in combination with the `DATE_FORMAT()` 
 * and the `STR_TO_DATE()` functions.
 * 
 * If format is `NULL`, this function returns `NULL`.
 * 
 * The possible values for the first and second arguments result in several possible format 
 * strings (for the specifiers used, see the table in the `DATE_FORMAT()` function description). 
 * 
 * `'ISO'` *format refers to ISO 9075, not ISO 8601.*
 * 
 * `TIMESTAMP` can also be used as the first argument to `GET_FORMAT()`, in which case the 
 * function returns the same values as for `DATETIME`.
 * 
 * ```SQL
 * mysql> SELECT DATE_FORMAT('2003-10-03',GET_FORMAT(DATE,'EUR'));
 *         -> '03.10.2003'
 * mysql> SELECT STR_TO_DATE('10.31.2003',GET_FORMAT(DATE,'USA'));
 *         -> '2003-10-31'
 * ```
 * #
 * | Function Call	                    | Result                |
 * |------------------------------------|-----------------------|
 * | `GET_FORMAT(DATE, 'USA')`          |	`'%m.%d.%Y'`          |
 * | `GET_FORMAT(DATE, 'JIS')`          |	`'%Y-%m-%d'`          |
 * | `GET_FORMAT(DATE, 'ISO')`          |	`'%Y-%m-%d'`          |
 * | `GET_FORMAT(DATE, 'EUR')`          |	`'%d.%m.%Y'`          |
 * | `GET_FORMAT(DATE, 'INTERNAL')`     |	`'%Y%m%d'`            |
 * | `GET_FORMAT(DATETIME, 'USA')`      |	`'%Y-%m-%d %H.%i.%s'` |
 * | `GET_FORMAT(DATETIME, 'JIS')`      |	`'%Y-%m-%d %H:%i:%s'` |
 * | `GET_FORMAT(DATETIME, 'ISO')`      |	`'%Y-%m-%d %H:%i:%s'` |
 * | `GET_FORMAT(DATETIME, 'EUR')`      |	`'%Y-%m-%d %H.%i.%s'` |
 * | `GET_FORMAT(DATETIME, 'INTERNAL')` |	`'%Y%m%d%H%i%s'`      |
 * | `GET_FORMAT(TIME, 'USA')`          |	`'%h:%i:%s %p'`       |
 * | `GET_FORMAT(TIME, 'JIS')`          |	`'%H:%i:%s'`          |
 * | `GET_FORMAT(TIME, 'ISO')`          |	`'%H:%i:%s'`          |
 * | `GET_FORMAT(TIME, 'EUR')`          |	`'%H.%i.%s'`          |
 * | `GET_FORMAT(TIME, 'INTERNAL')`     |	`'%H%i%s'`            |
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_get-format 
 */
export function get_format(value: Arg<date> | Arg<datetime> | Arg<time_stamp> | Arg<time>, format: 'EUR' | 'USA' | 'JIS' | 'ISO' | 'INTERNAL'): Col<string> {
  return new Col({
    defer(q, context) {
      return `GET_FORMAT(${q.colRef(value, context)}, ${q.colRef(format, context)})`;
    }
  });
}

/**
 * Returns the hour for `time`. The range of the return value is `0` to `23` for time-of-day 
 * values. However, the range of `TIME` values actually is much larger, so `HOUR` can 
 * return values greater than `23`. Returns `NULL` if time is `NULL`.
 * 
 * ```SQL
 * mysql> SELECT HOUR('10:05:03');
 *         -> 10
 * mysql> SELECT HOUR('272:59:59');
 *         -> 272
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_hour 
 */
export function hour(time: Arg<time>): Col<number> {
  return new Col({
    defer(q, context) {
      return `HOUR(${q.colRef(time, context)})`;
    }
  });
}

/**
 * Takes a `date` or `datetime` value and returns the corresponding value for the last day 
 * of the month. Returns `NULL` if the argument is invalid or `NULL`.
 * 
 * ```SQL
 * mysql> SELECT LAST_DAY('2003-02-05');
 *         -> '2003-02-28'
 * mysql> SELECT LAST_DAY('2004-02-05');
 *         -> '2004-02-29'
 * mysql> SELECT LAST_DAY('2004-01-01 01:01:01');
 *         -> '2004-01-31'
 * mysql> SELECT LAST_DAY('2003-03-32');
 *         -> NULL
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_last-day
 */
export function last_day(date: Arg<date | datetime>): Col<date> {
  return new Col({
    defer(q, context) {
      return `LAST_DAY(${q.colRef(date, context)})`;
    }
  });
}

/**
 * Internally proxies {@link now}
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_localtime
 */
export const localtime = now;

/**
 * Internally proxies {@link now}
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_localtimestamp
 */
export const localtimestamp = now;

/**
 * Returns a `date`, given `year` and `day_of_year` values. `day_of_year` must be greater 
 * than `0` or the result is `NULL`. The result is also `NULL` if either argument is `NULL`.
 * 
 * ```SQL
 * mysql> SELECT MAKEDATE(2011,31), MAKEDATE(2011,32);
 *         -> '2011-01-31', '2011-02-01'
 * mysql> SELECT MAKEDATE(2011,365), MAKEDATE(2014,365);
 *         -> '2011-12-31', '2014-12-31'
 * mysql> SELECT MAKEDATE(2011,0);
 *         -> NULL
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_makedate
 */
export function makedate(year: Arg<number>, day_of_year: Arg<number>): Col<date> {
  return new Col({
    defer(q, context) {
      return `MAKEDATE(${q.colRef(year, context)}, ${q.colRef(day_of_year, context)})`;
    }
  })
}

/**
 * Returns a time value calculated from the `hour`, `minute`, and `second` arguments. 
 * Returns `NULL` if any of its arguments are `NULL`.
 * 
 * The `second` argument can have a fractional part.
 * 
 * ```SQL
 * mysql> SELECT MAKETIME(12,15,30);
 *         -> '12:15:30'
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_maketime
 */
export function maketime(hour: Arg<number>, minute: Arg<number>, second: Arg<number>): Col<time> {
  return new Col({
    defer(q, context) {
      return `MAKETIME(${q.colRef(hour, context)}, ${q.colRef(minute, context)}, ${q.colRef(second, context)})`;
    }
  })
}

/**
 * Returns the microseconds from the `time` or `datetime` expression `expr` as a number in the range from
 * `0` to `999999`. Returns `NULL` if `expr` is NU`LL.
 * 
 * ```SQL
 * mysql> SELECT MICROSECOND('12:00:00.123456');
 *         -> 123456
 * mysql> SELECT MICROSECOND('2019-12-31 23:59:59.000010');
 *         -> 10
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_microsecond
 */
export function microsecond(expr: Arg<datetime> | Arg<time>): Col<number> {
  return new Col({
    defer(q, context) {
      return `MICROSECOND(${q.colRef(expr, context)})`;
    }
  })
}

/**
 * Returns the minute for `time`, in the range `0` to `59`, or `NULL` if time is `NULL`.
 * 
 * ```SQL
 * mysql> SELECT MINUTE('2008-02-03 10:05:03');
 *         -> 5
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_minute
 */
export function minute(time: Arg<datetime> | Arg<time>): Col<number> {
  return new Col({
    defer(q, context) {
      return `MINUTE(${q.colRef(time, context)})`;
    }
  })
}

/**
 * Returns the month for `date`, in the range `1` to `12` for January to December, or 
 * `0` for dates such as `'0000-00-00'` or `'2008-00-00'` that have a zero month part. 
 * Returns `NULL` if date is `NULL`.
 * 
 * ```SQL
 * mysql> SELECT MONTH('2008-02-03');
 *         -> 2
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_month
 */
export function month(date: Arg<datetime> | Arg<date>): Col<number> {
  return new Col({
    defer(q, context) {
      return `MONTH(${q.colRef(date, context)})`;
    }
  })
}

/**
 * Returns the full name of the month for `date`. The language used for the name is controlled by the 
 * value of the [lc_time_names][1] system variable ([Section 10.16, “MySQL Server Locale Support”][2]).
 * 
 * Returns `NULL` if date is `NULL`.
 * 
 * ```SQL
 * mysql> SELECT MONTHNAME('2008-02-03');
 *         -> 'February'
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_monthname
 * 
 * [1]: <https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_lc_time_names>
 * [2]: <https://dev.mysql.com/doc/refman/8.0/en/locale-support.html>
 */
export function monthname(date: Arg<datetime> | Arg<date>): Col<number> {
  return new Col({
    defer(q, context) {
      return `MONTHNAME(${q.colRef(date, context)})`;
    }
  })
}

/**
 * Returns the current date and time as a value in `'YYYY-MM-DD hh:mm:ss'` or `YYYYMMDDhhmmss` 
 * format, depending on whether the function is used in `string` or `numeric` context. 
 * The value is expressed in the session time zone.
 * 
 * If the `fsp` argument is given to specify a fractional seconds precision from `0` to `6`, 
 * the return value includes a fractional seconds part of that many digits.
 * 
 * ```SQL
 * mysql> SELECT NOW();
 *         -> '2007-12-15 23:50:26'
 * mysql> SELECT NOW() + 0;
 *         -> 20071215235026.000000
 * ```
 * 
 * `NOW()` returns a constant time that indicates the time at which the statement began to 
 * execute. (Within a stored function or trigger, `NOW()` returns the time at which the 
 * function or triggering statement began to execute.) This differs from the behavior for 
 * `SYSDATE()`, which returns the exact time at which it executes.
 * 
 * ```SQL
 * mysql> SELECT NOW(), SLEEP(2), NOW();
 * -- +---------------------+----------+---------------------+
 * -- | NOW()               | SLEEP(2) | NOW()               |
 * -- +---------------------+----------+---------------------+
 * -- | 2006-04-12 13:47:36 |        0 | 2006-04-12 13:47:36 |
 * -- +---------------------+----------+---------------------+
 * 
 * mysql> SELECT SYSDATE(), SLEEP(2), SYSDATE();
 * -- +---------------------+----------+---------------------+
 * -- | SYSDATE()           | SLEEP(2) | SYSDATE()           |
 * -- +---------------------+----------+---------------------+
 * -- | 2006-04-12 13:47:44 |        0 | 2006-04-12 13:47:46 |
 * -- +---------------------+----------+---------------------+
 * ```
 * 
 * In addition, the `SET TIMESTAMP` statement affects the value returned by `NOW()` 
 * but not by `SYSDATE()`. This means that timestamp settings in the binary log 
 * have no effect on invocations of `SYSDATE()`. Setting the timestamp to a nonzero 
 * value causes each subsequent invocation of `NOW()` to return that value. Setting 
 * the timestamp to zero cancels this effect so that `NOW()` once again returns the 
 * current date and time.
 * 
 * See the description for `SYSDATE()` for additional information about the differences 
 * between the two functions.
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_now
 * 
 * See also: 
 * - {@link sysdate}
 * 
 * @param fsp fractional seconds precision from `0` to `6`
 */
export function now<T extends (datetime | number) = datetime>(fsp?: Arg<0 | 1 | 2 | 3 | 4 | 5 | 6>): Col<T> {
  return new Col<T>({
    defer(q, context) {
      return fsp === undefined ? `NOW(${q.colRef(fsp, context)})` : 'NOW()';
    }
  });
}

/**
 * Adds `N` months to period `P` (in the format `YYMM` or `YYYYMM`). Returns a value in the format `YYYYMM`.
 * 
 * The period argument `P` is not a date value.
 * 
 * This function returns `NULL` if `P` or `N` is `NULL`.
 * 
 * ```SQL
 * mysql> SELECT PERIOD_ADD(200801, 2);
 *         -> 200803
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_period-add
 */
export function period_add(p: Arg<number>, n: Arg<number>): Col<number> {
  return new Col({
    defer(q, context) {
      return `PERIOD_ADD(${q.colRef(p, context)}, ${q.colRef(n, context)})`;
    }
  })
}

/**
 * Returns the number of months between periods `P1` and `P2`. `P1` and `P2` should be in 
 * the format `YYMM` or `YYYYMM`. Note that the period arguments `P1` and `P2` are not 
 * date values.
 * 
 * This function returns NULL if P1 or P2 is NULL.
 * 
 * ```SQL
 * mysql> SELECT PERIOD_DIFF(200802, 200703);
 *         -> 11
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_period-diff
 */
export function period_diff(p1: Arg<number>, p2: Arg<number>): Col<number> {
  return new Col({
    defer(q, context) {
      return `PERIOD_DIFF(${q.colRef(p1, context)}, ${q.colRef(p2, context)})`;
    }
  })
}

/**
 * Returns the quarter of the year for `date`, in the range `1` to `4`, or `NULL` if date is `NULL`.
 * 
 * ```SQL
 * mysql> SELECT QUARTER('2008-04-01');
 *         -> 2
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_quarter
 */
export function quarter(date: Arg<datetime> | Arg<date>): Col<number> {
  return new Col({
    defer(q, context) {
      return `QUARTER(${q.colRef(date, context)})`;
    }
  })
}

/**
 * Returns the second for `time`, in the range `0` to `59`, or `NULL` if time is `NULL`.
 * 
 * ```SQL
 * mysql> SELECT SECOND('10:05:03');
 *         -> 3
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_second
 */
export function second(time: Arg<datetime> | Arg<time>): Col<number> {
  return new Col({
    defer(q, context) {
      return `SECOND(${q.colRef(time, context)})`;
    }
  })
}

/**
 * Returns the `seconds` argument, converted to hours, minutes, and seconds, as a `TIME` value. The range 
 * of the result is constrained to that of the `TIME` data type. A warning occurs if the argument 
 * corresponds to a value outside that range.
 * 
 * The function returns NULL if seconds is NULL.
 * 
 * ```SQL
 * mysql> SELECT SEC_TO_TIME(2378);
 *         -> '00:39:38'
 * mysql> SELECT SEC_TO_TIME(2378) + 0;
 *         -> 3938
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_sec-to-time
 */
export function sec_to_time(seconds: Arg<number>): Col<time> {
  return new Col({
    defer(q, context) {
      return `SEC_TO_TIME(${q.colRef(seconds, context)})`;
    }
  })
}

/**
 * This is the inverse of the `DATE_FORMAT()` function. It takes a string `str` and a format string `format`. 
 * `STR_TO_DATE()` returns a `DATETIME` value if the format string contains both date and time parts, or a 
 * `DATE` or `TIME` value if the string contains only date or time parts. 
 * 
 * If `str` or `format` are `NULL`, the function returns `NULL`. If the date, time, or datetime value extracted 
 * from `str` is illegal, `STR_TO_DATE()` returns `NULL` and produces a warning.
 * 
 * The server scans `str` attempting to match format to it. The `format` string can contain literal characters 
 * and format specifiers beginning with `%`. Literal characters in format must match literally in `str`. Format 
 * specifiers in `format` must match a date or time part in `str`. For the specifiers that can be used in format, 
 * see the `DATE_FORMAT()` function description.
 * 
 * 
 * ```SQL
 * mysql> SELECT STR_TO_DATE('01,5,2013','%d,%m,%Y');
 *         -> '2013-05-01'
 * mysql> SELECT STR_TO_DATE('May 1, 2013','%M %d,%Y');
 *         -> '2013-05-01'
 * ```
 * 
 * Scanning starts at the beginning of `str` and fails if `format` is found not to match. Extra characters at the 
 * end of str are ignored.
 * 
 * ```SQL
 * mysql> SELECT STR_TO_DATE('a09:30:17','a%h:%i:%s');
 *         -> '09:30:17'
 * mysql> SELECT STR_TO_DATE('a09:30:17','%h:%i:%s');
 *         -> NULL
 * mysql> SELECT STR_TO_DATE('09:30:17a','%h:%i:%s');
 *         -> '09:30:17'
 * ```
 * 
 * Unspecified date or time parts have a value of `0`, so incompletely specified values in `str` produce a result
 * with some or all parts set to `0`:
 * 
 * ```SQL
 * mysql> SELECT STR_TO_DATE('abc','abc');
 *         -> '0000-00-00'
 * mysql> SELECT STR_TO_DATE('9','%m');
 *         -> '0000-09-00'
 * mysql> SELECT STR_TO_DATE('9','%s');
 *         -> '00:00:09'
 * ```
 * 
 * Range checking on the parts of date values is as described in [Section 11.2.2, “The DATE, DATETIME, and TIMESTAMP Types”][1]. 
 * This means, for example, that “zero” dates or dates with part values of 0 are permitted unless the SQL mode is 
 * set to disallow such values.
 * 
 * ```SQL
 * mysql> SELECT STR_TO_DATE('00/00/0000', '%m/%d/%Y');
 *         -> '0000-00-00'
 * mysql> SELECT STR_TO_DATE('04/31/2004', '%m/%d/%Y');
 *         -> '2004-04-31'
 * ```
 * 
 * If the [`NO_ZERO_DATE`][2] SQL mode is enabled, zero dates are disallowed. In that case, `STR_TO_DATE()` returns `NULL` 
 * and generates a warning:
 * 
 * ```SQL
 * mysql> SET sql_mode = '';
 * mysql> SELECT STR_TO_DATE('00/00/0000', '%m/%d/%Y');
 * --      +---------------------------------------+
 * --      | STR_TO_DATE('00/00/0000', '%m/%d/%Y') |
 * --      +---------------------------------------+
 * --      | 0000-00-00                            |
 * --      +---------------------------------------+
 * mysql> SET sql_mode = 'NO_ZERO_DATE';
 * mysql> SELECT STR_TO_DATE('00/00/0000', '%m/%d/%Y');
 * --      +---------------------------------------+
 * --      | STR_TO_DATE('00/00/0000', '%m/%d/%Y') |
 * --      +---------------------------------------+
 * --      | NULL                                  |
 * --      +---------------------------------------+
 * mysql> SHOW WARNINGS\G
 * --      *************************** 1. row ***************************
 * --        Level: Warning
 * --         Code: 1411
 * --      Message: Incorrect datetime value: '00/00/0000' for function str_to_date
 * ```
 * 
 * **Note**
 * 
 * You cannot use format `"%X%V"` to convert a year-week string to a date because the combination of 
 * a year and week does not uniquely identify a year and month if the week crosses a month boundary. 
 * To convert a year-week to a date, you should also specify the weekday:
 * 
 * ```SQL
 * mysql> SELECT STR_TO_DATE('200442 Monday', '%X%V %W');
 *         -> '2004-10-18'
 * ```
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_str-to-date
 * 
 * See also: 
 * - {@link date_format}
 * 
 * [1]: <https://dev.mysql.com/doc/refman/8.0/en/datetime.html>
 * [2]: <https://dev.mysql.com/doc/refman/8.0/en/sql-mode.html#sqlmode_no_zero_date>
 */
export function str_to_date<T extends (date | datetime | time) = string>(str: Arg<string>, format: Arg<string>): Col<T> {
  return new Col({
    defer(q, context) {
      return `STR_TO_DATE(${q.colRef(str, context)}, ${q.colRef(format, context)})`;
    }
  })
}

/**
 * When invoked with the `INTERVAL` form of the second argument, `SUBDATE()` is a synonym for `DATE_SUB()`. For 
 * information on the `INTERVAL` unit argument, see the discussion for `DATE_ADD()`.
 * 
 * ```SQL
 * mysql> SELECT DATE_SUB('2008-01-02', INTERVAL 31 DAY);
 *         -> '2007-12-02'
 * mysql> SELECT SUBDATE('2008-01-02', INTERVAL 31 DAY);
 *         -> '2007-12-02'
 * ```
 * 
 * The second form enables the use of an integer value for `days`. In such cases, it is interpreted as the 
 * number of days to be subtracted from the date or datetime expression `expr`.
 * 
 * ```SQL
 * mysql> SELECT SUBDATE('2008-01-02 12:00:00', 31);
 *         -> '2007-12-02 12:00:00'
 * ```
 * 
 * This function returns `NULL` if any of its arguments are `NULL`.
 * 
 * Ref: https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_subdate
 * 
 * See also: 
 * - {@link date_sub}
 * - {@link date_add}
 */
export function subdate(expr: Arg<date>, days: Arg<number>): Col<date>;
export function subdate(date: Arg<date>, unit: ArgMap<TemporalIntervalObject>): Col<date>;
export function subdate(date: Arg<date>, arg: Arg<number> | ArgMap<TemporalIntervalObject>): Col<date> {

  if (typeof arg === 'number' || arg instanceof Col)
    return new Col<date>({
      defer(q, context) {
        return `SUBDATE(${q.colRef(date, context)}, ${q.colRef(arg, context)})`;
      }
    });

  else
    return new Col<date>({
      defer(q, context) {
        return Object.entries(arg).reduceRight((out, [key, value]) => {

          const interval = TEMPORAL_INTERVAL_MAP.get(key) ?? (() => { throw new Error(`Key ${key} is does not match any TemporalIntervalObject key`) })();

          return `SUBDATE(${out}, INTERVAL ${q.colRef(value, context)} ${interval})`;

        }, q.colRef(date, context));
      }
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


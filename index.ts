let _PTR = 1;
let _MAX_PTR = Number.MAX_SAFE_INTEGER;

function nextPtr() {
  const ret = _PTR++;
  if (_PTR > _MAX_PTR)
    _PTR = 1;
  return ret;
}

type EscapeFn = (value: any) => string;

let escape: EscapeFn = (value: any) => {
  throw new Error('Must register an escape function');
}

export function registerEscaper(fn: EscapeFn) {
  escape = fn;
}

export function registerMaxPtr(int: number) {
  _MAX_PTR = int;
}

export class Table<T> {
  public _base: T;
  constructor(
    public name: string,
    public base: new () => T
  ) {
    this._base = new this.base();
  }
}

type QColMap<T> = { [Property in keyof T]: QCol<T[Property]> }
type QExpressionMap<T> = { [Property in keyof T]: ExpressionArgTyped<T[Property]> }

type QTableMap<T> = { _base_: T; _alias_: string; _name_: string; } & QColMap<T>;

class QTable<T> {

  constructor(
    public _base_: T,
    public _name_: string,
    public _alias_: string,
    public _cols_?: QColMap<T>
  ) {
    if (!this._cols_)
      this._cols_ = Object.fromEntries(Object.entries(_base_).map(pair => [pair[0], new QCol<unknown>('', `${_alias_}.${pair[0]}`)])) as any;

    Object.entries(this._cols_!).forEach((pair) => (this as any)[pair[0]] = pair[1]);

  }

}

class QCol<TValue> {
  private _useAlias = false;

  constructor(
    private _alias: string,
    public path: string,
    public bindings?: BindingMap
  ) { }

  get alias() {
    return this._useAlias ? this._alias : '';
  }

  set alias(value: string) {
    this._alias = value;
  }

  asAlias(alias = '') {
    this._useAlias = true;

    if (alias)
      this._alias = alias;

    return this;
  }
}

class QJoinable<T> {

  constructor(
    protected table: QTable<T>,
    protected _q_: QueryBuilder
  ) { }

  innerJoin<J, TKey, U extends Record<string, QColMap<unknown>>>(
    table: Table<J>,
    t1On: (obj: QColMap<T>) => QCol<TKey>,
    t2On: (obj: QColMap<J>) => QCol<TKey>,
    map: (t1: QColMap<T>, t2: QColMap<J>) => U): QSelectableJoin<U> {

    const model: Record<string, QColMap<T>> = { foo: this.table._cols_! };
    const _t1On = (_m: Record<string, QColMap<T>>) => t1On(model.foo);
    const _map = (_t1: Record<string, QColMap<T>>, _t2: QColMap<J>) => map(model.foo, _t2)

    return _joinBase(model, this._q_, table, _t1On, t2On, _map, 'INNER');
  }

  leftJoin<J, TKey, U extends Record<string, QColMap<unknown>>>(
    table: Table<J>,
    t1On: (obj: QColMap<T>) => QCol<TKey>,
    t2On: (obj: QColMap<J>) => QCol<TKey>,
    map: (t1: QColMap<T>, t2: QColMap<J>) => U): QSelectableJoin<U> {

    const model: Record<string, QColMap<T>> = { foo: this.table._cols_! };
    const _t1On = (_m: Record<string, QColMap<T>>) => t1On(model.foo);
    const _map = (_t1: Record<string, QColMap<T>>, _t2: QColMap<J>) => map(model.foo, _t2)

    return _joinBase(model, this._q_, table, _t1On, t2On, _map, 'LEFT');
  }

  rightJoin<J, TKey, U extends Record<string, QColMap<unknown>>>(
    table: Table<J>,
    t1On: (obj: QColMap<T>) => QCol<TKey>,
    t2On: (obj: QColMap<J>) => QCol<TKey>,
    map: (t1: QColMap<T>, t2: QColMap<J>) => U): QSelectableJoin<U> {

    const model: Record<string, QColMap<T>> = { foo: this.table._cols_! };
    const _t1On = (_m: Record<string, QColMap<T>>) => t1On(model.foo);
    const _map = (_t1: Record<string, QColMap<T>>, _t2: QColMap<J>) => map(model.foo, _t2)

    return _joinBase(model, this._q_, table, _t1On, t2On, _map, 'RIGHT');
  }

  crossJoin<J, TKey, U extends Record<string, QColMap<unknown>>>(
    table: Table<J>,
    t1On: (obj: QColMap<T>) => QCol<TKey>,
    t2On: (obj: QColMap<J>) => QCol<TKey>,
    map: (t1: QColMap<T>, t2: QColMap<J>) => U): QSelectableJoin<U> {

    const model: Record<string, QColMap<T>> = { foo: this.table._cols_! };
    const _t1On = (_m: Record<string, QColMap<T>>) => t1On(model.foo);
    const _map = (_t1: Record<string, QColMap<T>>, _t2: QColMap<J>) => map(model.foo, _t2)

    return _joinBase(model, this._q_, table, _t1On, t2On, _map, 'CROSS');
  }

}

class QSelectable<T> extends QJoinable<T> {

  constructor(
    table: QTable<T>,
    _q_: QueryBuilder
  ) {
    super(table, _q_);
  }

  select<U extends Record<string, QCol<unknown>>>(fn: (tables: QColMap<T>) => U) {

    const model: Record<string, QColMap<T>> = { foo: this.table._cols_! };
    const _fn = (tables: Record<string, QColMap<unknown>>) => fn(model.foo);
    return _selectBase(model, this._q_, _fn);
  }


  toSql() {
    return this._q_.toSql();
  }
}

class QInsertable<T>{

  constructor(
    private table: QTable<T>,
    private _q_: QueryBuilder
  ) { }

  values(...values: (QExpressionMap<T> | QSelected<QColMap<T>, Record<string, QColMap<unknown>>>)[]) {

    if (values.length) {

      for (const map of values) {

        if (map instanceof QSelected) {

          if (!this._q_.cols)
            this._q_.cols = map['_selected_'];

        }
        else {
          for (const key in map) {
            if (!map[key].alias)
              map[key].alias = key;
          }

          if (!this._q_.cols)
            this._q_.cols = map;
        }

        this._q_.insertValues.push(map);
      }

    }

    return new QInserted(this._q_);
  }

  toSql() {
    return this._q_.toSql();
  }
}

class QJoinableJoin<TModel extends Record<string, QColMap<unknown>>>{
  constructor(
    protected _model_: TModel,
    protected _q_: QueryBuilder
  ) { }

  leftJoin<J, TKey, U extends Record<string, QColMap<unknown>>>
    (
      table: Table<J>,
      t1On: (obj: TModel) => QCol<TKey>,
      t2On: (obj: QColMap<J>) => QCol<TKey>,
      map: (t1: TModel, t2: QColMap<J>) => U): QSelectableJoin<U> {

    return _joinBase(this._model_, this._q_, table, t1On, t2On, map, 'LEFT');
  }

  innerJoin<J, TKey, U extends Record<string, QColMap<unknown>>>
    (
      table: Table<J>,
      t1On: (obj: TModel) => QCol<TKey>,
      t2On: (obj: QColMap<J>) => QCol<TKey>,
      map: (t1: TModel, t2: QColMap<J>) => U): QSelectableJoin<U> {

    return _joinBase(this._model_, this._q_, table, t1On, t2On, map, 'INNER');
  }

  crossJoin<J, TKey, U extends Record<string, QColMap<unknown>>>
    (
      table: Table<J>,
      t1On: (obj: TModel) => QCol<TKey>,
      t2On: (obj: QColMap<J>) => QCol<TKey>,
      map: (t1: TModel, t2: QColMap<J>) => U): QSelectableJoin<U> {

    return _joinBase(this._model_, this._q_, table, t1On, t2On, map, 'CROSS');
  }

  rightJoin<J, TKey, U extends Record<string, QColMap<unknown>>>
    (
      table: Table<J>,
      t1On: (obj: TModel) => QCol<TKey>,
      t2On: (obj: QColMap<J>) => QCol<TKey>,
      map: (t1: TModel, t2: QColMap<J>) => U): QSelectableJoin<U> {

    return _joinBase(this._model_, this._q_, table, t1On, t2On, map, 'RIGHT');
  }
}

class QSelectableJoin<TModel extends Record<string, QColMap<unknown>>> extends QJoinableJoin<TModel> {

  constructor(
    _model_: TModel,
    _q_: QueryBuilder
  ) {
    super(_model_, _q_);
  }

  select<U extends Record<string, QCol<unknown>>>(fn: (model: TModel) => U): QTypeSelected<U, TModel> {
    return _selectBase(this._model_, this._q_, fn);
  }

}

function _joinBase<TModel extends Record<string, QColMap<unknown>>, J, TKey, U extends Record<string, QColMap<unknown>>>
  (
    model: TModel,
    q: QueryBuilder,
    table: Table<J>,
    t1On: (obj: TModel) => QCol<TKey>,
    t2On: (obj: QColMap<J>) => QCol<TKey>,
    map: (t1: TModel, t2: QColMap<J>) => U,
    key: 'INNER' | 'LEFT' | 'RIGHT' | 'CROSS'): QSelectableJoin<U> {

  const join = new QTable<J>(table._base, table.name, `T${nextPtr()}`);

  q.joins.push(new Operation(`${key} JOIN ${join._name_} ${join._alias_} ON ${t1On(model).path} = ${t2On(join._cols_!).path}`));

  const mapped = map(model, join._cols_!);

  return new QSelectableJoin(mapped, q);
}

function _selectBase<TModel extends Record<string, QColMap<unknown>>, U extends Record<string, QCol<unknown>>>(
  model: TModel,
  q: QueryBuilder,
  fn: (model: TModel) => U): QSelected<U, TModel> {

  const selected = fn(model);

  for (const key in selected)
    if (!selected[key].alias)
      selected[key].alias = key;

  q.cols = selected;

  return new QSelected(selected, q, model);
}

function _insertBase<TModel extends Record<string, QColMap<unknown>>, U extends Record<string, QCol<unknown>>>(
  model: TModel,
  q: QueryBuilder,
  fn: (model: TModel) => U[]) {

  const maps = fn(model);


  if (maps.length) {

    for (const map of maps) {
      for (const key in map)
        if (!map[key].alias)
          map[key].alias = key;

      q.insertValues.push(map);
    }

    const cols = maps[0];

    q.cols = cols;

  }

  return new QInserted(q);

}

type QTypeSelected<T, U> = QSelected<T, U>;

class QSelected<ColMap extends QColMap<unknown>, TSource> {
  constructor(
    private _selected_: ColMap,
    private _q_: QueryBuilder,
    private _model_: TSource
  ) { }

  where(fn: (obj: ColMap, source: TSource) => Operation) {
    const op = fn(this._selected_, this._model_);
    this._q_.whereClauses.push(op);
    return this;
  }

  groupBy(fn: (obj: ColMap, source: TSource) => QCol<unknown>) {
    const key = fn(this._selected_, this._model_);
    const op = new Operation(`GROUP BY ${key.path}`);
    this._q_.groupBy = op;
    return this;
  }

  having(fn: (obj: ColMap, source: TSource) => Operation) {
    const op = fn(this._selected_, this._model_);
    this._q_.havingClauses.push(op);
    return this;
  }

  limit(number: number) {
    this._q_.limit = number;
    return this;
  }

  offset(number: number) {
    this._q_.offset = number;
    return this;
  }

  orderBy<ArgType extends { on: QCol<unknown> | Operation, direction?: 'ASC' | 'DESC' }>(fn: (obj: ColMap, source: TSource) => ArgType[] | ArgType) {

    const val = fn(this._selected_, this._model_);

    const stmts: ArgType[] = (Array.isArray(val) ? val : [val]) || [];

    let strings: string[] = [];
    let bindings: { [index: string]: any } = {};

    if (stmts.length)
      for (const obj of stmts) {
        const direction = obj.direction || 'ASC';

        if (obj.on instanceof QCol) {
          strings.push(`${obj.on.alias || obj.on.path} ${direction}`);
        }
        else if (obj.on instanceof Operation) {
          strings.push(`${obj.on.str} ${direction}`);
          if (obj.on.bindings)
            bindings = { ...bindings, ...obj.on.bindings };
        }
      }

    this._q_.orderBys.push(new Operation(strings.join(', '), Object.keys(bindings).length ? bindings : undefined));

    return this;
  }

  toSql() {
    return this._q_.toSql();
  }

  asCol(alias = '') {
    const path = `(${this.toSql()})`;
    return new QCol(alias, path).asAlias();
  }

}

class QInserted {
  constructor(private _q_: QueryBuilder) { }

  toSql() {
    return this._q_.toSql();
  }
}

class QueryBuilder {

  whereClauses: Operation[] = [];
  havingClauses: Operation[] = [];
  joins: Operation[] = [];
  groupBy?: Operation;
  limit = Infinity;
  offset = 0;
  orderBys: Operation[] = [];

  insertValues: (Record<string, QCol<unknown>> | QSelected<QColMap<unknown>, unknown>)[] = [];

  constructor(
    public from: QTable<any>,
    public type: 'SELECT' | 'INSERT' | 'UPDATE' | 'DELETE',
    public cols?: QColMap<any>,
  ) { }

  toSql() {
    if (!this.cols)
      throw new Error('No columns set');

    let q = '';

    const bindings = [...this.whereClauses, ...this.joins, ...this.havingClauses, ...Object.values(this.cols)]
      .filter(x => !!x.bindings)
      .map(x => x.bindings!);

    if (bindings.length) {
      for (const binding of bindings)
        for (const key in binding)
          q += `SET @${key} = ${escape(binding[key])};\r\n`;
    }

    switch (this.type) {
      case 'SELECT': {
        q += `SELECT ${Object.entries(this.cols).map(pair => {

          let str = `${pair[1].path}`;

          if (pair[1].alias)
            str += ` AS ${pair[1].alias}`;

          return str;

        }).join(', ')}\r\nFROM ${this.from._name_} ${this.from._alias_}`;
        break;
      }

      case 'INSERT': {
        const keys = Object.keys(this.cols);
        const values = this.insertValues.map(o => {

          const orderedVals: QCol<unknown>[] = [];

          if (o instanceof QSelected) {
            orderedVals.push(o.asCol());
          } else {
            for (const key of keys)
              orderedVals.push(o[key]);
          }

          return `(${orderedVals.map(col => col.alias || col.path).join(', ')})`;

        });
        q += `INSERT INTO ${this.from._name_} ${this.from._alias_}\r\n(${keys.map(key => `${this.from._alias_}.${key}`).join(', ')})\r\nVALUES\r\n${values.join(',\r\n')}`;
      }
    }


    if (this.joins.length)
      q += `\r\n${this.joins.map(op => op.str).join('\n')}`;

    if (this.whereClauses.length)
      q += `\r\nWHERE ${this.whereClauses.map(clause => `(${clause.str})`).join(' AND ')}`;

    if (this.groupBy)
      q += `\r\n${this.groupBy.str}`;

    if (this.havingClauses.length)
      q += `\r\nHAVING ${this.havingClauses.map(clause => `(${clause.str})`).join(' AND ')}`;

    if (this.orderBys.length)
      q += `\r\nORDER BY ${this.orderBys.map(x => x.str).join(', ')}`;

    if (this.limit < Infinity && typeof this.limit === 'number')
      q += `\r\nLIMIT ${this.limit}`;

    if (this.offset > 0 && typeof this.offset === 'number')
      q += `\r\nOFFSET ${this.offset}`;

    return q;
  }
}

type BindingMap = { [index: string]: any };

class Operation {
  constructor(
    public str: string,
    public bindings?: BindingMap
  ) { }

  asCol(alias = '') {
    const path = `(${this.str})`;
    return new QCol(alias, path, this.bindings).asAlias();
  }
}

type ExpressionArg = QCol<unknown> | Operation | number | string;
type ExpressionArgTyped<T> = QCol<T> | Operation | T;

function _paramaterize(arg: QCol<unknown> | Operation | number | string) {
  let param = '';
  let binding: BindingMap | undefined = undefined;

  if (arg instanceof QCol) {
    param = arg.alias || arg.path;
    binding = arg.bindings;
  } else if (arg instanceof Operation) {
    param = arg.str;
    binding = arg.bindings;
  } else if (typeof arg === 'string' || typeof arg === 'number') {
    const key = `value_${nextPtr()}`;
    binding = { [key]: arg };
    param = `@${key}`;
  }

  return { param, binding };
}

export function from<T>(table: Table<T>): QSelectable<T> {
  const qTable = new QTable(table._base, table.name, `T${nextPtr()}`);

  return new QSelectable<T>(qTable, new QueryBuilder(qTable, 'SELECT', qTable._cols_!));
}

export function insertInto<T>(table: Table<T>) {
  const qTable = new QTable(table._base, table.name, `T${nextPtr()}`);

  return new QInsertable<T>(qTable, new QueryBuilder(qTable, 'INSERT'));
}

function _symbolOperation(col: ExpressionArg, value: ExpressionArg, symbol: '<' | '>' | '=' | '<>' | '<=' | '>=') {

  let bindings: BindingMap = {};
  let left = '';
  let right = '';

  {
    const { param, binding } = _paramaterize(col);
    left = param;
    if (binding)
      bindings = { ...bindings, ...binding };
  }

  {
    const { param, binding } = _paramaterize(value);
    right = param;
    if (binding)
      bindings = { ...bindings, ...binding };
  }

  return new Operation(`${left} ${symbol} ${right}`, Object.keys(bindings).length ? bindings : undefined);

}

export function equals(col: ExpressionArg, value: ExpressionArg) {
  return _symbolOperation(col, value, '=');
}

export function notEquals(col: ExpressionArg, value: ExpressionArg) {
  return _symbolOperation(col, value, '<>');
}

export function greaterThan(col: ExpressionArg, value: ExpressionArg) {
  return _symbolOperation(col, value, '>');
}

export function lessThan(col: ExpressionArg, value: ExpressionArg) {
  return _symbolOperation(col, value, '<');
}

export function greaterThanOrEqualTo(col: ExpressionArg, value: ExpressionArg) {
  return _symbolOperation(col, value, '>=');
}

export function lessThanOrEqualTo(col: ExpressionArg, value: ExpressionArg) {
  return _symbolOperation(col, value, '<=');
}

function _andOrBase(args: ExpressionArg[], keyword: 'AND' | 'OR') {
  let strings: string[] = [];
  let bindings: { [index: string]: any } = {};

  if (args.length)
    for (const arg of args) {
      const { param, binding } = _paramaterize(arg);
      strings.push(param);
      if (binding)
        bindings = { ...bindings, ...binding };
    }

  return new Operation(`(${strings.join(` ${keyword} `)})`, Object.keys(bindings).length ? bindings : undefined);
}

export function and(...ops: ExpressionArg[]) {
  return _andOrBase(ops, 'AND');
}

export function or(...ops: ExpressionArg[]) {
  return _andOrBase(ops, 'OR');
}

export function not(arg: ExpressionArg) {
  const { param, binding } = _paramaterize(arg);
  return new Operation(`NOT(${param})`, binding);
}

function _aggregateNumberFnBase(arg: ExpressionArg, key: 'COUNT' | 'MAX' | 'MIN' | 'AVG' | 'SUM') {
  const { param, binding } = _paramaterize(arg);
  let str = `${key}(${param})`;
  return new QCol<number>('', str, binding);
}

export function count(arg: ExpressionArg) {
  return _aggregateNumberFnBase(arg, 'COUNT');
}

export function max(arg: ExpressionArg) {
  return _aggregateNumberFnBase(arg, 'MAX');
}

export function min(arg: ExpressionArg) {
  return _aggregateNumberFnBase(arg, 'MIN');
}

export function avg(arg: ExpressionArg) {
  return _aggregateNumberFnBase(arg, 'AVG');
}

export function sum(arg: ExpressionArg) {
  return _aggregateNumberFnBase(arg, 'SUM');
}

function _arithmeticBase(symbol: '+' | '-' | '/' | '*' | '%', args: ExpressionArg[]) {
  let bindings: BindingMap = {};
  let strings: string[] = [];

  if (args.length)
    for (const arg of args) {
      const { param, binding } = _paramaterize(arg);
      strings.push(param);
      if (binding)
        bindings = { ...bindings, ...binding };
    }

  return new Operation(`${strings.join(` ${symbol} `)}`, Object.keys(bindings).length ? bindings : undefined);
}

export function add(...args: ExpressionArg[]) {
  return _arithmeticBase('+', args);
}

export function subtract(...args: ExpressionArg[]) {
  return _arithmeticBase('-', args);
}

export function multiply(...args: ExpressionArg[]) {
  return _arithmeticBase('*', args);
}

export function divide(...args: ExpressionArg[]) {
  return _arithmeticBase('/', args);
}

export function modulo(...args: ExpressionArg[]) {
  return _arithmeticBase('%', args);
}

function _bitwiseBase(symbol: '&' | '|' | '^', args: ExpressionArg[]) {
  let bindings: BindingMap = {};
  let strings: string[] = [];

  if (args.length)
    for (const arg of args) {
      const { param, binding } = _paramaterize(arg);
      strings.push(param);
      if (binding)
        bindings = { ...bindings, ...binding };
    }

  return new Operation(`${strings.join(` ${symbol} `)}`, Object.keys(bindings).length ? bindings : undefined);
}

export function bitwiseAnd(...args: ExpressionArg[]) {
  return _bitwiseBase('&', args);
}

export function bitwiseOr(...args: ExpressionArg[]) {
  return _bitwiseBase('|', args);
}

export function bitwiseOrInclusive(...args: ExpressionArg[]) {
  return _bitwiseBase('^', args);
}

export function like(col: ExpressionArg, arg: ExpressionArg) {
  let bindings: { [index: string]: any } = {};
  let source = '';
  let expression = '';

  {
    const { param, binding } = _paramaterize(arg);
    source = param;
    if (binding)
      bindings = { ...bindings, ...binding };
  }

  {
    const { param, binding } = _paramaterize(arg);
    expression = param;
    if (binding)
      bindings = { ...bindings, ...binding };
  }

  return new Operation(`${source} LIKE ${expression}`, Object.keys(bindings).length ? bindings : undefined);
}

export function concat(...args: ExpressionArg[]) {
  let strings: string[] = [];
  let bindings: { [index: string]: any } = {};

  if (args.length)
    for (const arg of args) {
      const { param, binding } = _paramaterize(arg);
      strings.push(param);

      if (binding)
        bindings = { ...bindings, ...binding };
    }

  return new Operation(`CONCAT(${strings.join(', ')})`, Object.keys(bindings).length ? bindings : undefined);
}

export function isIn(col: ExpressionArg, ...args: ExpressionArg[]) {

  let strings: string[] = [];
  let bindings: BindingMap = {};
  let source = '';

  {
    const { param, binding } = _paramaterize(col);
    source = param;
    if (binding)
      bindings = { ...bindings, ...binding };
  }

  if (args.length)
    for (const arg of args) {
      const { param, binding } = _paramaterize(arg);
      strings.push(param);
      if (binding)
        if (binding)
          bindings = { ...bindings, ...binding };
    }

  return new Operation(`${source} IN (${strings.join(', ')})`, Object.keys(bindings).length ? bindings : undefined);

}

export function between(arg: ExpressionArg, lowerLimit: ExpressionArg, upperLimit: ExpressionArg): Operation {

  let bindings: BindingMap = {};
  let lower = '';
  let upper = '';
  let source = '';

  {
    const { param, binding } = _paramaterize(arg);
    source = param;
    if (binding)
      bindings = { ...bindings, ...binding };
  }

  {
    const { param, binding } = _paramaterize(lowerLimit);
    lower = param;
    if (binding)
      bindings = { ...bindings, ...binding };
  }

  {
    const { param, binding } = _paramaterize(upperLimit);
    upper = param;
    if (binding)
      bindings = { ...bindings, ...binding };
  }

  return new Operation(`${source} BETWEEN ${lower} AND ${upper}`, bindings);
}

export function raw<T>(value: T) {
  return new QCol<T>('', escape(value));
}

export function isNotNull(arg: ExpressionArg) {
  const { param, binding } = _paramaterize(arg);
  return new Operation(`${param} IS NOT NULL`, binding);
}

export function isNull(arg: ExpressionArg) {
  const { param, binding } = _paramaterize(arg);
  return new Operation(`${param} IS NULL`, binding);
}

export function coalesce(...args: ExpressionArg[]) {
  let bindings: BindingMap = {};
  let strings: string[] = [];

  if (args.length)
    for (const arg of args) {
      const { param, binding } = _paramaterize(arg);
      strings.push(param);
      if (binding)
        bindings = { ...bindings, ...binding };
    }

  return new Operation(`COALESCE(${strings.join(', ')})`, Object.keys(bindings).length ? bindings : undefined);
}

export function exists(arg: QTypeSelected<unknown, unknown>) {
  return new Operation(`EXISTS(${arg.toSql()})`);
}

export function any(arg: ExpressionArg, operator: '=' | '<' | '>' | '<>' | '<=' | '>=', subQuery: QTypeSelected<unknown, unknown>) {
  const { param, binding } = _paramaterize(arg);
  return new Operation(`${param} ${operator} ANY (${subQuery.toSql()})`, binding);
}

export function all(arg: ExpressionArg, operator: '=' | '<' | '>' | '<>' | '<=' | '>=', subQuery: QTypeSelected<unknown, unknown>) {
  const { param, binding } = _paramaterize(arg);
  return new Operation(`${param} ${operator} ALL (${subQuery.toSql()})`, binding);
}

export function caseFn(...args: { when: ExpressionArg, then: ExpressionArg }[]) {
  let bindings: BindingMap = {};

  return new Operation(`\r\n  (CASE\r\n${args.map(arg => {

    let when = '';
    let then = '';

    {
      const { param, binding } = _paramaterize(when);
      when = param;
      if (binding)
        bindings = { ...bindings, ...binding };
    }

    {
      const { param, binding } = _paramaterize(then);
      then = param;
      if (binding)
        bindings = { ...bindings, ...binding };
    }

    return `    WHEN ${when} THEN ${then}`;

  }).join('\r\n')}\r\n  END)\r\n`, bindings);
}

export function ifNull(expression: ExpressionArg, alt_value: ExpressionArg) {
  let bindings: BindingMap = {};

  const exp = _paramaterize(expression);
  if (exp.binding)
    bindings = { ...bindings, ...exp.binding };

  const alt = _paramaterize(alt_value);
  if (alt.binding)
    bindings = { ...bindings, ...alt.binding };

  return new Operation(`IFNULL(${exp.param}, ${alt.param})`, bindings);
}

export function groupConcat(arg: ExpressionArg) {
  const { param, binding } = _paramaterize(arg);
  return new Operation(`GROUP_CONCAT(${param})`, binding);
}

import { Table, Query, from, registerEscaper, insertInto, abs, update, deleteFrom, isEqualTo, add, multiply, subtract, isGreaterThan, raw, count } from './index';
import { escape } from 'mysql2';

registerEscaper(escape);

class Foo {
  id: number = 0;
  name: string = '';
  deleted = false;
}

class Bar {
  id: number = 0;
  value: string = '';
}

class FooBar {
  id: number = 0;
  foo_id = 0;
  bar_id = 0;
}

const foos = new Table('foos', Foo);
const bars = new Table('bars', Bar);
const foobars = new Table('foobars', FooBar);

console.log('start')
console.time();


// const query = insertInto(foos)
//   .values(
//     from(bars).select(o => ({
//       id: o.id,
//       name: o.value,
//       deleted: raw(false)
//     }))
//   )
//   .toSql()

const query = from(foos)
  .innerJoin(foobars, t1 => t1.id, t2 => t2.foo_id, (foo, foobars) => ({ foo, foobars }))
  .select(o => ({
    ...o.foo,
    bars: count(o.foobars.id)
  }))
  .groupBy((o) => o.id)
  .toSql();

console.timeEnd();
console.log(query);
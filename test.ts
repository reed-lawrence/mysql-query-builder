import { Table, Query, from, registerEscaper, insertInto, abs, update, deleteFrom } from './index';
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

const query = deleteFrom(foos)
.toSql();


console.timeEnd();
console.log(query);
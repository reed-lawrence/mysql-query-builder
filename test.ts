import { equals, from, insertInto, raw, registerEscaper, Table } from './index';
import { escape } from 'mysql2';

registerEscaper(escape);

class Foo {
  id: number = 0;
  name: string = '';
}

class Bar {
  id: number = 0;
  bar_name: string = '';
}

const foos = new Table('foos', Foo);
const bars = new Table('bars', Bar);

const query = insertInto(foos)
  .values(
    {
      id: 1,
      name: ''
    }
  )
  .toSql();

console.log(query);
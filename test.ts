import { Table, Query, from, registerEscaper, insertInto, abs, update, deleteFrom, equalTo, add, multiply, subtract, greaterThan, raw, count, subquery, SelectResult, Col, QColMap, and, match, or, greatest, soundex, date_add, adddate } from './index';
import { escape } from 'mysql2';
import { performance, PerformanceObserver } from 'node:perf_hooks';


registerEscaper(escape);

class Post {
  id: number = 0;
  name: string = '';
  date_created: Date = new Date(0);
  deleted = false;
}

class Tag {
  id: number = 0;
  value: string = '';
  deleted = false;

  post_id: number = 0;
}


const posts = new Table('posts', Post);
const tags = new Table('tags', Tag);



const query = from(posts)
  .select(o => ({ ...o, modified_date: adddate('2021-01-01', 1) }));

console.log(query.toSql());




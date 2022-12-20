import { Table, Query, from, registerEscaper, insertInto, abs, update, deleteFrom, equalTo, add, multiply, subtract, greaterThan, raw, count, subquery, SelectResult, Col, QColMap, and, match, or, greatest, soundex } from './index';
import { escape } from 'mysql2';
import { performance, PerformanceObserver } from 'node:perf_hooks';


registerEscaper(escape);

class Post {
  id: number = 0;
  name: string = '';
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



const query = from(tags)
  .innerJoin(
    from(posts).select(o => o).limit(25),
    t1 => t1.post_id,
    t2 => t2.id,
    (tag, post) => ({ tag, post })
  ).select((o) => ({
    post_id: o.post.id,
    post_name: o.post.name,
    post_deleted: o.post.deleted,

    r1: equalTo(o.post.name, 'hello world'),
    r2: match([o.post.name], { against: 'hello world', in: 'IN NATURAL LANGUAGE MODE' }),

    tag_id: o.tag.id,
    tag_value: o.tag.value,
    tag_deleted: o.tag.deleted
  }))
  .where(o => equalTo(o.post_deleted, false))
  .having(o => or(
    equalTo(o.r1, 1),
    greaterThan(o.r2, 0)
  ))
  .orderBy(o => [{ col: o.r1, direction: 'desc' }, { col: o.r2, direction: 'desc' }, { col: o.post_id, direction: 'desc' }]);

console.log(query.toSql());




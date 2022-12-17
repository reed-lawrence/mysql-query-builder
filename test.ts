import { Table, Query, from, registerEscaper, insertInto, abs, update, deleteFrom, isEqualTo, add, multiply, subtract, isGreaterThan, raw, count, subquery, SelectResult, QCol, QColMap, and, match, or, greatest } from './index';
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

console.time();

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

    r1: isEqualTo(o.post.name, 'hello world'),
    r2: match([o.post.name], { against: 'hello world', in: 'IN NATURAL LANGUAGE MODE' }),

    tag_id: o.tag.id,
    tag_value: o.tag.value,
    tag_deleted: o.tag.deleted
  }))
  .where(o => isEqualTo(o.post_deleted, false))
  .having(o => or(
    isEqualTo(o.r1, 1),
    isGreaterThan(o.r2, 0)
  ))
  .orderBy(o => [{ col: o.r1, direction: 'desc' }, { col: o.r2, direction: 'desc' }, { col: o.post_id, direction: 'desc' }])
  .toSql();

console.timeEnd();


import { Table, Query, from, registerEscaper, insertInto, abs, update, deleteFrom, isEqualTo, add, multiply, subtract, isGreaterThan, raw, count, subquery, SelectResult, QCol, QColMap } from './index';
import { escape } from 'mysql2';

registerEscaper(escape);

class Post {
  id: number = 0;
  name: string = '';
  deleted = false;
}

class Tag {
  id: number = 0;
  value: string = '';

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

    tag_id: o.tag.id,
    tag_value: o.tag.value
  })).where(o => isEqualTo(o.post_deleted, false))
  .toSql();

console.log(query);

type Callable = {
  (...args: any[]): string;
}

type Is = Callable & {
  not: Callable;
  greater: {
    than: Callable;
    or: Equality
  }
};

type Equality = {
  than: Callable
  or: Equality;
  less: Equality;
}

type ColExt<T> = QCol<T> & {
  is: Is;
  in: any;
  equals: Callable;
};

let foo: ColExt<string> = {} as any;

foo.is.greater.or.less.than(15);
foo.is.not(null);
foo.equals(1);
foo.in


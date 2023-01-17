import { escape } from 'mysql2';
import { add, adddate, and, Col, count, deleteFrom, equalTo, from, greaterThan, insertInto, QColMap, QSelected, registerEscaper, subquery, Table, update } from './index';

registerEscaper(escape);

class Post {
  id: number = 0;
  name: string = '';
  date_created: Date = new Date(0);
  likes: number = 0;
  deleted = false;
}

class Tag {
  id: number = 0;
  value: string = '';
  deleted = false;

  post_id: number = 0;
}

class PostImages {
  post_id = 0;
  file_id = 0;
}

class File {
  id = 0;
  name = '';
  type = 0;
  deleted = false;
}

class FileType {
  id = 0;
  mimetype = '';
}


const posts = new Table('posts', Post);
const tags = new Table('tags', Tag);
const post_images = new Table('post_images', PostImages);
const files = new Table('files', File);
const file_types = new Table('file_types', FileType);

// const query = from(tags)
//   .leftJoin(
//     from(post_images)
//       .innerJoin(files, o => o.file_id, o => o.id, (join, file) => ({ join, file }))
//       .innerJoin(file_types, o => o.file.type, o => o.id, (model, file_type) => ({ ...model, file_type }))
//       .select(o => ({
//         ...o.file,
//         post_id: o.join.post_id,
//         mimetype: o.file_type.mimetype,
//         testfield: 'hello world'
//       })),
//     o => o.post_id, o => o.post_id, (tag, image) => ({ tag, image })
//   )
//   .innerJoin(
//     from(posts)
//       .select(o => o)
//       .where(o => equalTo(o.deleted, false))
//       .limit(25)
//       .offset(0),
//     o => o.tag.post_id, o => o.id, (model, post) => ({ ...model, post })
//   )
//   .select(o => ({
//     ...o.post,

//     tag_value: o.tag.value,

//     file_id: o.image.id,
//     file_name: o.image.name,
//     file_type: o.image.type,
//     file_mimetype: o.image.mimetype,

//   }))
//   .where((o, model) => and(
//     equalTo(model.image.deleted, false),
//     equalTo(model.tag.deleted, false)
//   ));

// const query = from(posts)
//   .innerJoin(tags, o => o.id, o => o.post_id, (post, tag) => ({ post, tag }))
//   .select((o) => ({
//     ...o.post,
//     tag_id: o.tag.id,
//     tag_value: o.tag.value
//   }))
//   .where((_, o) => equalTo(o.tag.deleted, false));

// const query = insertInto(files)
//   .values({
//     name: 'test_name',
//     type: subquery(
//       from(file_types)
//         .select(o => 1)
//         .where((_, o) => equalTo(o.mimetype, 'image/png'))
//     )
//   });

// const query = deleteFrom(posts)
//   .where(o => equalTo(o.id, 100));

const query = update(posts).set((o) => ({
  deleted: false,
  likes: add(o.likes, 1)
})).where(o => equalTo(o.id, 100));

console.log(query.toSql());




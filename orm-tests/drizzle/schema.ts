import {
  pgTable,
  serial,
  varchar,
  integer,
  boolean,
  text,
  jsonb,
  uuid,
  timestamp,
  index,
  uniqueIndex,
  primaryKey,
} from 'drizzle-orm/pg-core';
import { relations } from 'drizzle-orm';

export const drizzleUsers = pgTable(
  'drizzle_users',
  {
    id: serial('id').primaryKey(),
    email: varchar('email', { length: 255 }).notNull().unique(),
    name: varchar('name', { length: 100 }).notNull(),
    age: integer('age').default(0),
    isActive: boolean('is_active').default(true),
    bio: text('bio'),
    metadata: jsonb('metadata'),
    externalId: uuid('external_id'),
    createdAt: timestamp('created_at', { withTimezone: true }).defaultNow(),
    updatedAt: timestamp('updated_at', { withTimezone: true }).defaultNow(),
  },
  (table) => ({
    emailIdx: uniqueIndex('drizzle_users_email_idx').on(table.email),
  })
);

export const drizzlePosts = pgTable(
  'drizzle_posts',
  {
    id: serial('id').primaryKey(),
    title: varchar('title', { length: 500 }).notNull(),
    content: text('content').notNull(),
    published: boolean('published').default(false),
    viewCount: integer('view_count').default(0),
    settings: jsonb('settings'),
    createdAt: timestamp('created_at', { withTimezone: true }).defaultNow(),
    authorId: integer('author_id')
      .notNull()
      .references(() => drizzleUsers.id, { onDelete: 'cascade' }),
  },
  (table) => ({
    titleIdx: index('drizzle_posts_title_idx').on(table.title),
  })
);

export const drizzleTags = pgTable(
  'drizzle_tags',
  {
    id: serial('id').primaryKey(),
    name: varchar('name', { length: 100 }).notNull().unique(),
    color: varchar('color', { length: 7 }).default('#000000'),
  },
  (table) => ({
    nameIdx: uniqueIndex('drizzle_tags_name_idx').on(table.name),
  })
);

export const drizzlePostTags = pgTable(
  'drizzle_post_tags',
  {
    postId: integer('post_id')
      .notNull()
      .references(() => drizzlePosts.id, { onDelete: 'cascade' }),
    tagId: integer('tag_id')
      .notNull()
      .references(() => drizzleTags.id, { onDelete: 'cascade' }),
  },
  (table) => ({
    pk: primaryKey({ columns: [table.postId, table.tagId] }),
  })
);

export const usersRelations = relations(drizzleUsers, ({ many }) => ({
  posts: many(drizzlePosts),
}));

export const postsRelations = relations(drizzlePosts, ({ one, many }) => ({
  author: one(drizzleUsers, {
    fields: [drizzlePosts.authorId],
    references: [drizzleUsers.id],
  }),
  postTags: many(drizzlePostTags),
}));

export const tagsRelations = relations(drizzleTags, ({ many }) => ({
  postTags: many(drizzlePostTags),
}));

export const postTagsRelations = relations(drizzlePostTags, ({ one }) => ({
  post: one(drizzlePosts, {
    fields: [drizzlePostTags.postId],
    references: [drizzlePosts.id],
  }),
  tag: one(drizzleTags, {
    fields: [drizzlePostTags.tagId],
    references: [drizzleTags.id],
  }),
}));

export type User = typeof drizzleUsers.$inferSelect;
export type NewUser = typeof drizzleUsers.$inferInsert;
export type Post = typeof drizzlePosts.$inferSelect;
export type NewPost = typeof drizzlePosts.$inferInsert;
export type Tag = typeof drizzleTags.$inferSelect;
export type NewTag = typeof drizzleTags.$inferInsert;

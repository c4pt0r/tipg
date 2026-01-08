import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { Sequelize } from 'sequelize';
import { createSequelize } from './connection.js';
import { User, Post, Tag } from './models.js';

describe('Sequelize Relations & Joins [pg-tikv]', () => {
  let sequelize: Sequelize;

  beforeAll(async () => {
    sequelize = createSequelize();
    await sequelize.sync({ force: true });
  });

  afterAll(async () => {
    await sequelize.drop();
    await sequelize.close();
  });

  beforeEach(async () => {
    await sequelize.query('DELETE FROM sequelize_post_tags');
    await Post.destroy({ where: {}, force: true });
    await Tag.destroy({ where: {}, force: true });
    await User.destroy({ where: {}, force: true });
  });

  describe('one-to-many relations', () => {
    let userId: number;

    beforeEach(async () => {
      const user = await User.create({
        email: 'author@example.com',
        name: 'Author',
        age: 30,
      });
      userId = user.id;

      await Post.bulkCreate([
        { title: 'Post 1', content: 'Content 1', authorId: user.id },
        { title: 'Post 2', content: 'Content 2', authorId: user.id },
        { title: 'Post 3', content: 'Content 3', authorId: user.id },
      ]);
    });

    it('should eager load one-to-many relation', async () => {
      const user = await User.findByPk(userId, {
        include: [{ model: Post, as: 'posts' }],
      });

      expect(user).not.toBeNull();
      expect(user?.posts).toHaveLength(3);
      user?.posts?.forEach((post) => {
        expect(post.authorId).toBe(userId);
      });
    });

    it('should load relation with include condition', async () => {
      await Post.update({ published: true }, { where: { title: 'Post 1' } });

      const user = await User.findByPk(userId, {
        include: [{ model: Post, as: 'posts', where: { published: true } }],
      });

      expect(user?.posts).toHaveLength(1);
      expect(user?.posts?.[0].title).toBe('Post 1');
    });

    it('should lazy load relation', async () => {
      const user = await User.findByPk(userId);
      const posts = await Post.findAll({ where: { authorId: user!.id } });

      expect(posts).toHaveLength(3);
    });
  });

  describe('many-to-one relations', () => {
    it('should load many-to-one relation', async () => {
      const user = await User.create({
        email: 'author2@example.com',
        name: 'Author 2',
        age: 25,
      });

      await Post.create({
        title: 'My Post',
        content: 'Content',
        authorId: user.id,
      });

      const post = await Post.findOne({
        where: { title: 'My Post' },
        include: [{ model: User, as: 'author' }],
      });

      expect(post?.author).not.toBeNull();
      expect(post?.author?.name).toBe('Author 2');
    });
  });

  describe('many-to-many relations', () => {
    let postId: number;

    beforeEach(async () => {
      const user = await User.create({
        email: 'tagger@example.com',
        name: 'Tagger',
        age: 30,
      });

      const tags = await Tag.bulkCreate([
        { name: 'typescript', color: '#3178c6' },
        { name: 'nodejs', color: '#68a063' },
        { name: 'database', color: '#336791' },
      ]);

      const post = await Post.create({
        title: 'Tagged Post',
        content: 'Content with tags',
        authorId: user.id,
      });
      postId = post.id;

      await (post as Post & { setTags: (tags: Tag[]) => Promise<void> }).setTags([tags[0], tags[1]]);
    });

    it('should load many-to-many relation', async () => {
      const post = await Post.findByPk(postId, {
        include: [{ model: Tag, as: 'tags' }],
      });

      expect(post?.tags).toHaveLength(2);
      const tagNames = post?.tags?.map((t) => t.name).sort();
      expect(tagNames).toEqual(['nodejs', 'typescript']);
    });

    it('should add to many-to-many relation', async () => {
      const post = await Post.findByPk(postId);
      const dbTag = await Tag.findOne({ where: { name: 'database' } });

      await (post as Post & { addTag: (tag: Tag) => Promise<void> }).addTag(dbTag!);

      const updated = await Post.findByPk(postId, {
        include: [{ model: Tag, as: 'tags' }],
      });

      expect(updated?.tags).toHaveLength(3);
    });

    it('should remove from many-to-many relation', async () => {
      const post = await Post.findByPk(postId);
      const nodejsTag = await Tag.findOne({ where: { name: 'nodejs' } });

      await (post as Post & { removeTag: (tag: Tag) => Promise<void> }).removeTag(nodejsTag!);

      const updated = await Post.findByPk(postId, {
        include: [{ model: Tag, as: 'tags' }],
      });

      expect(updated?.tags).toHaveLength(1);
      expect(updated?.tags?.[0].name).toBe('typescript');
    });

    it('should query through many-to-many relation', async () => {
      const posts = await Post.findAll({
        include: [{
          model: Tag,
          as: 'tags',
          where: { name: 'typescript' },
        }],
      });

      expect(posts).toHaveLength(1);
      expect(posts[0].title).toBe('Tagged Post');
    });
  });

  describe('nested includes', () => {
    it('should load multiple levels of relations', async () => {
      const user = await User.create({
        email: 'nested@example.com',
        name: 'Nested User',
        age: 35,
      });

      const tag = await Tag.create({ name: 'nested-tag', color: '#ff0000' });

      const post = await Post.create({
        title: 'Nested Post',
        content: 'Content',
        authorId: user.id,
      });

      await (post as Post & { addTag: (tag: Tag) => Promise<void> }).addTag(tag);

      const foundUser = await User.findByPk(user.id, {
        include: [{
          model: Post,
          as: 'posts',
          include: [{ model: Tag, as: 'tags' }],
        }],
      });

      expect(foundUser?.posts).toHaveLength(1);
      expect(foundUser?.posts?.[0].tags).toHaveLength(1);
      expect(foundUser?.posts?.[0].tags?.[0].name).toBe('nested-tag');
    });
  });

  describe('join types', () => {
    beforeEach(async () => {
      const user1 = await User.create({
        email: 'haspost@example.com',
        name: 'Has Post',
        age: 30,
      });

      await User.create({
        email: 'nopost@example.com',
        name: 'No Post',
        age: 25,
      });

      await Post.create({
        title: 'Single Post',
        content: 'Content',
        authorId: user1.id,
      });
    });

    it('should perform LEFT JOIN (include without required)', async () => {
      const users = await User.findAll({
        include: [{ model: Post, as: 'posts', required: false }],
        order: [['name', 'ASC']],
      });

      expect(users).toHaveLength(2);
      expect(users[0].name).toBe('Has Post');
      expect(users[0].posts).toHaveLength(1);
      expect(users[1].name).toBe('No Post');
      expect(users[1].posts).toHaveLength(0);
    });

    it('should perform INNER JOIN (include with required)', async () => {
      const users = await User.findAll({
        include: [{ model: Post, as: 'posts', required: true }],
      });

      expect(users).toHaveLength(1);
      expect(users[0].name).toBe('Has Post');
    });
  });
});

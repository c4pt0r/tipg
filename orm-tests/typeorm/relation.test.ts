import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { DataSource } from 'typeorm';
import { createDataSource } from './datasource.js';
import { User, Post, Tag } from './entities/index.js';

describe('TypeORM Relations & Joins [pg-tikv]', () => {
  let dataSource: DataSource;

  beforeAll(async () => {
    dataSource = createDataSource({ synchronize: true });
    await dataSource.initialize();
  });

  afterAll(async () => {
    if (dataSource?.isInitialized) {
      await dataSource.query('DROP TABLE IF EXISTS typeorm_post_tags CASCADE');
      await dataSource.query('DROP TABLE IF EXISTS typeorm_posts CASCADE');
      await dataSource.query('DROP TABLE IF EXISTS typeorm_tags CASCADE');
      await dataSource.query('DROP TABLE IF EXISTS typeorm_users CASCADE');
      await dataSource.destroy();
    }
  });

  beforeEach(async () => {
    await dataSource.query('DELETE FROM typeorm_post_tags');
    await dataSource.query('DELETE FROM typeorm_posts');
    await dataSource.query('DELETE FROM typeorm_tags');
    await dataSource.query('DELETE FROM typeorm_users');
  });

  describe('one-to-many relations', () => {
    let userId: number;

    beforeEach(async () => {
      const userRepo = dataSource.getRepository(User);
      const postRepo = dataSource.getRepository(Post);

      const user = await userRepo.save({
        email: 'author@example.com',
        name: 'Author',
        age: 30,
      });
      userId = user.id;

      await postRepo.save([
        { title: 'Post 1', content: 'Content 1', authorId: user.id },
        { title: 'Post 2', content: 'Content 2', authorId: user.id },
        { title: 'Post 3', content: 'Content 3', authorId: user.id },
      ]);
    });

    it('should eager load one-to-many relation', async () => {
      const user = await dataSource
        .getRepository(User)
        .findOne({
          where: { id: userId },
          relations: ['posts'],
        });

      expect(user).not.toBeNull();
      expect(user?.posts).toHaveLength(3);
      user?.posts.forEach((post) => {
        expect(post.authorId).toBe(userId);
      });
    });

    it('should load relation with QueryBuilder', async () => {
      const user = await dataSource
        .getRepository(User)
        .createQueryBuilder('user')
        .leftJoinAndSelect('user.posts', 'post')
        .where('user.id = :id', { id: userId })
        .getOne();

      expect(user?.posts).toHaveLength(3);
    });

    it('should filter on related entity', async () => {
      const postRepo = dataSource.getRepository(Post);
      await postRepo.update({ title: 'Post 1' }, { published: true });

      const user = await dataSource
        .getRepository(User)
        .createQueryBuilder('user')
        .leftJoinAndSelect('user.posts', 'post', 'post.published = :pub', { pub: true })
        .where('user.id = :id', { id: userId })
        .getOne();

      expect(user?.posts).toHaveLength(1);
      expect(user?.posts[0].title).toBe('Post 1');
    });
  });

  describe('many-to-one relations', () => {
    it('should load many-to-one relation', async () => {
      const userRepo = dataSource.getRepository(User);
      const postRepo = dataSource.getRepository(Post);

      const user = await userRepo.save({
        email: 'author2@example.com',
        name: 'Author 2',
        age: 25,
      });

      await postRepo.save({
        title: 'My Post',
        content: 'Content',
        authorId: user.id,
      });

      const post = await postRepo.findOne({
        where: { title: 'My Post' },
        relations: ['author'],
      });

      expect(post?.author).not.toBeNull();
      expect(post?.author.name).toBe('Author 2');
    });
  });

  describe('many-to-many relations', () => {
    let postId: number;

    beforeEach(async () => {
      const userRepo = dataSource.getRepository(User);
      const postRepo = dataSource.getRepository(Post);
      const tagRepo = dataSource.getRepository(Tag);

      const user = await userRepo.save({
        email: 'tagger@example.com',
        name: 'Tagger',
        age: 30,
      });

      const tags = await tagRepo.save([
        { name: 'typescript', color: '#3178c6' },
        { name: 'nodejs', color: '#68a063' },
        { name: 'database', color: '#336791' },
      ]);

      const post = postRepo.create({
        title: 'Tagged Post',
        content: 'Content with tags',
        authorId: user.id,
        tags: [tags[0], tags[1]],
      });
      const savedPost = await postRepo.save(post);
      postId = savedPost.id;
    });

    it('should load many-to-many relation', async () => {
      const post = await dataSource
        .getRepository(Post)
        .findOne({
          where: { id: postId },
          relations: ['tags'],
        });

      expect(post?.tags).toHaveLength(2);
      const tagNames = post?.tags.map((t) => t.name).sort();
      expect(tagNames).toEqual(['nodejs', 'typescript']);
    });

    it('should add to many-to-many relation', async () => {
      const postRepo = dataSource.getRepository(Post);
      const tagRepo = dataSource.getRepository(Tag);

      const post = await postRepo.findOne({
        where: { id: postId },
        relations: ['tags'],
      });

      const dbTag = await tagRepo.findOneBy({ name: 'database' });
      post!.tags.push(dbTag!);
      await postRepo.save(post!);

      const updated = await postRepo.findOne({
        where: { id: postId },
        relations: ['tags'],
      });

      expect(updated?.tags).toHaveLength(3);
    });

    it('should remove from many-to-many relation', async () => {
      const postRepo = dataSource.getRepository(Post);

      const post = await postRepo.findOne({
        where: { id: postId },
        relations: ['tags'],
      });

      post!.tags = post!.tags.filter((t) => t.name !== 'nodejs');
      await postRepo.save(post!);

      const updated = await postRepo.findOne({
        where: { id: postId },
        relations: ['tags'],
      });

      expect(updated?.tags).toHaveLength(1);
      expect(updated?.tags[0].name).toBe('typescript');
    });

    it('should query through many-to-many relation', async () => {
      const posts = await dataSource
        .getRepository(Post)
        .createQueryBuilder('post')
        .innerJoinAndSelect('post.tags', 'tag')
        .where('tag.name = :tagName', { tagName: 'typescript' })
        .getMany();

      expect(posts).toHaveLength(1);
      expect(posts[0].title).toBe('Tagged Post');
    });
  });

  describe('nested relations', () => {
    it('should load multiple levels of relations', async () => {
      const userRepo = dataSource.getRepository(User);
      const postRepo = dataSource.getRepository(Post);
      const tagRepo = dataSource.getRepository(Tag);

      const user = await userRepo.save({
        email: 'nested@example.com',
        name: 'Nested User',
        age: 35,
      });

      const tag = await tagRepo.save({ name: 'nested-tag', color: '#ff0000' });

      const post = postRepo.create({
        title: 'Nested Post',
        content: 'Content',
        authorId: user.id,
        tags: [tag],
      });
      await postRepo.save(post);

      const foundUser = await userRepo.findOne({
        where: { id: user.id },
        relations: ['posts', 'posts.tags'],
      });

      expect(foundUser?.posts).toHaveLength(1);
      expect(foundUser?.posts[0].tags).toHaveLength(1);
      expect(foundUser?.posts[0].tags[0].name).toBe('nested-tag');
    });
  });

  describe('join types', () => {
    beforeEach(async () => {
      const userRepo = dataSource.getRepository(User);
      const postRepo = dataSource.getRepository(Post);

      const user1 = await userRepo.save({
        email: 'haspost@example.com',
        name: 'Has Post',
        age: 30,
      });

      await userRepo.save({
        email: 'nopost@example.com',
        name: 'No Post',
        age: 25,
      });

      await postRepo.save({
        title: 'Single Post',
        content: 'Content',
        authorId: user1.id,
      });
    });

    it('should perform LEFT JOIN', async () => {
      const users = await dataSource
        .getRepository(User)
        .createQueryBuilder('user')
        .leftJoinAndSelect('user.posts', 'post')
        .orderBy('user.name', 'ASC')
        .getMany();

      expect(users).toHaveLength(2);
      expect(users[0].name).toBe('Has Post');
      expect(users[0].posts).toHaveLength(1);
      expect(users[1].name).toBe('No Post');
      expect(users[1].posts).toHaveLength(0);
    });

    it('should perform INNER JOIN', async () => {
      const users = await dataSource
        .getRepository(User)
        .createQueryBuilder('user')
        .innerJoinAndSelect('user.posts', 'post')
        .getMany();

      expect(users).toHaveLength(1);
      expect(users[0].name).toBe('Has Post');
    });
  });
});

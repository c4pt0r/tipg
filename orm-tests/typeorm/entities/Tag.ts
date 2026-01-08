import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  ManyToMany,
  Index,
} from 'typeorm';
import { Post } from './Post.js';

@Entity('typeorm_tags')
export class Tag {
  @PrimaryGeneratedColumn()
  id!: number;

  @Column({ type: 'varchar', length: 100, unique: true })
  @Index()
  name!: string;

  @Column({ type: 'varchar', length: 7, default: '#000000' })
  color!: string;

  @ManyToMany(() => Post, (post) => post.tags)
  posts!: Post[];
}

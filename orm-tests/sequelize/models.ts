import { Sequelize, DataTypes, Model, Optional } from 'sequelize';

interface UserAttributes {
  id: number;
  email: string;
  name: string;
  age: number;
  isActive: boolean;
  bio: string | null;
  metadata: Record<string, unknown> | null;
  externalId: string | null;
  createdAt: Date;
  updatedAt: Date;
}

interface UserCreationAttributes extends Optional<UserAttributes, 'id' | 'age' | 'isActive' | 'bio' | 'metadata' | 'externalId' | 'createdAt' | 'updatedAt'> {}

export class User extends Model<UserAttributes, UserCreationAttributes> implements UserAttributes {
  declare id: number;
  declare email: string;
  declare name: string;
  declare age: number;
  declare isActive: boolean;
  declare bio: string | null;
  declare metadata: Record<string, unknown> | null;
  declare externalId: string | null;
  declare createdAt: Date;
  declare updatedAt: Date;
  declare posts?: Post[];
}

interface PostAttributes {
  id: number;
  title: string;
  content: string;
  published: boolean;
  viewCount: number;
  settings: Record<string, unknown> | null;
  createdAt: Date;
  authorId: number;
}

interface PostCreationAttributes extends Optional<PostAttributes, 'id' | 'published' | 'viewCount' | 'settings' | 'createdAt'> {}

export class Post extends Model<PostAttributes, PostCreationAttributes> implements PostAttributes {
  declare id: number;
  declare title: string;
  declare content: string;
  declare published: boolean;
  declare viewCount: number;
  declare settings: Record<string, unknown> | null;
  declare createdAt: Date;
  declare authorId: number;
  declare author?: User;
  declare tags?: Tag[];
}

interface TagAttributes {
  id: number;
  name: string;
  color: string;
}

interface TagCreationAttributes extends Optional<TagAttributes, 'id' | 'color'> {}

export class Tag extends Model<TagAttributes, TagCreationAttributes> implements TagAttributes {
  declare id: number;
  declare name: string;
  declare color: string;
  declare posts?: Post[];
}

export function initModels(sequelize: Sequelize): void {
  User.init(
    {
      id: { type: DataTypes.INTEGER, autoIncrement: true, primaryKey: true },
      email: { type: DataTypes.STRING(255), allowNull: false, unique: true },
      name: { type: DataTypes.STRING(100), allowNull: false },
      age: { type: DataTypes.INTEGER, defaultValue: 0 },
      isActive: { type: DataTypes.BOOLEAN, defaultValue: true },
      bio: { type: DataTypes.TEXT, allowNull: true },
      metadata: { type: DataTypes.JSONB, allowNull: true },
      externalId: { type: DataTypes.UUID, allowNull: true },
      createdAt: { type: DataTypes.DATE, defaultValue: DataTypes.NOW },
      updatedAt: { type: DataTypes.DATE, defaultValue: DataTypes.NOW },
    },
    { sequelize, tableName: 'sequelize_users', timestamps: true }
  );

  Post.init(
    {
      id: { type: DataTypes.INTEGER, autoIncrement: true, primaryKey: true },
      title: { type: DataTypes.STRING(500), allowNull: false },
      content: { type: DataTypes.TEXT, allowNull: false },
      published: { type: DataTypes.BOOLEAN, defaultValue: false },
      viewCount: { type: DataTypes.INTEGER, defaultValue: 0 },
      settings: { type: DataTypes.JSONB, allowNull: true },
      createdAt: { type: DataTypes.DATE, defaultValue: DataTypes.NOW },
      authorId: { type: DataTypes.INTEGER, allowNull: false },
    },
    { sequelize, tableName: 'sequelize_posts', timestamps: false }
  );

  Tag.init(
    {
      id: { type: DataTypes.INTEGER, autoIncrement: true, primaryKey: true },
      name: { type: DataTypes.STRING(100), allowNull: false, unique: true },
      color: { type: DataTypes.STRING(7), defaultValue: '#000000' },
    },
    { sequelize, tableName: 'sequelize_tags', timestamps: false }
  );

  User.hasMany(Post, { foreignKey: 'authorId', as: 'posts', onDelete: 'CASCADE' });
  Post.belongsTo(User, { foreignKey: 'authorId', as: 'author' });

  Post.belongsToMany(Tag, { through: 'sequelize_post_tags', as: 'tags' });
  Tag.belongsToMany(Post, { through: 'sequelize_post_tags', as: 'posts' });
}

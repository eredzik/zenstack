import { createTestClient } from '@zenstackhq/testtools';
import { afterEach, describe, expect, it } from 'vitest';

const schema = `
model User {
    id String @id
    email String @unique
    posts Post[]
    comments Comment[]
}

model Post {
    id String @id
    sequence Int
    title String
    author User @relation(fields: [authorId], references: [id])
    authorId String
    comments Comment[]
}

model Comment {
    id String @id
    content String
    post Post @relation(fields: [postId], references: [id])
    postId String
    author User? @relation(fields: [authorId], references: [id])
    authorId String?
}
`;

function makePostsData(count: number) {
    return Array.from({ length: count }, (_, i) => {
        const sequence = count - i; // insert descending
        return {
            id: `p${sequence}`,
            sequence,
            title: `P${sequence}`,
            // Keep outer relation (User -> posts) required.
            authorId: 'u1',
        };
    });
}

function makeCommentsData(count: number) {
    return Array.from({ length: count }, (_, i) => {
        const sequence = count - i;
        return {
            id: `c${sequence}`,
            postId: `p${sequence}`,
            content: `C${sequence}`,
            // Make nested to-one include nullable to vary lateral join execution.
            authorId: sequence % 11 === 0 ? null : 'u1',
        };
    });
}

describe('Relation orderBy with nested includes', () => {
    let db: any;

    afterEach(async () => {
        await db?.$disconnect();
    });

    it('keeps stable order for to-many include with nested includes', async () => {
        const count = 2000;

        db = await createTestClient(schema);

        await db.user.create({ data: { id: 'u1', email: 'u1@example.com' } });
        await db.post.createMany({ data: makePostsData(count) });
        await db.comment.createMany({ data: makeCommentsData(count) });

        const user = await db.user.findFirst({
            where: { id: 'u1' },
            include: {
                posts: {
                    orderBy: { sequence: 'asc' },
                    include: { author: true, comments: { include: { author: true } } },
                },
            },
        });

        const ascSequences = user.posts.map((p: any) => p.sequence);
        expect(ascSequences).toEqual(Array.from({ length: count }, (_, i) => i + 1));

        const userDesc = await db.user.findFirst({
            where: { id: 'u1' },
            include: {
                posts: {
                    orderBy: { sequence: 'desc' },
                    include: { author: true, comments: { include: { author: true } } },
                },
            },
        });

        const descSequences = userDesc.posts.map((p: any) => p.sequence);
        expect(descSequences).toEqual(Array.from({ length: count }, (_, i) => count - i));
    });

    it('keeps stable order for to-many select with nested selects', async () => {
        const count = 2000;

        db = await createTestClient(schema);

        await db.user.create({ data: { id: 'u1', email: 'u1@example.com' } });
        await db.post.createMany({ data: makePostsData(count) });
        await db.comment.createMany({ data: makeCommentsData(count) });

        const user = await db.user.findFirst({
            where: { id: 'u1' },
            select: {
                id: true,
                posts: {
                    orderBy: { sequence: 'asc' },
                    select: {
                        sequence: true,
                        author: { select: { id: true } },
                        comments: { select: { author: { select: { id: true } } } },
                    },
                },
            },
        });

        const ascSequences = user.posts.map((p: any) => p.sequence);
        expect(ascSequences).toEqual(Array.from({ length: count }, (_, i) => i + 1));

        const userDesc = await db.user.findFirst({
            where: { id: 'u1' },
            select: {
                id: true,
                posts: {
                    orderBy: { sequence: 'desc' },
                    select: {
                        sequence: true,
                        author: { select: { id: true } },
                        comments: { select: { author: { select: { id: true } } } },
                    },
                },
            },
        });

        const descSequences = userDesc.posts.map((p: any) => p.sequence);
        expect(descSequences).toEqual(Array.from({ length: count }, (_, i) => count - i));
    });

    it('supports parallel user.comments + posts.comments with mixin-style scalars (regression benchmark shape)', async () => {
        db = await createTestClient(schema);

        await db.user.create({ data: { id: 'u1', email: 'u1@example.com' } });
        await db.post.createMany({
            data: [
                { id: 'p2', sequence: 2, title: 'P2', authorId: 'u1' },
                { id: 'p1', sequence: 1, title: 'P1', authorId: 'u1' },
            ],
        });
        await db.comment.createMany({
            data: [
                { id: 'c1', postId: 'p1', content: 'on p1', authorId: 'u1' },
                { id: 'c2', postId: 'p2', content: 'on p2', authorId: 'u1' },
            ],
        });

        const row = await db.user.findUnique({
            where: { id: 'u1' },
            include: {
                posts: {
                    orderBy: [{ sequence: 'desc' }, { id: 'asc' }],
                    include: {
                        comments: {
                            orderBy: [{ id: 'asc' }],
                            include: { author: true },
                        },
                    },
                },
                comments: {
                    orderBy: [{ content: 'desc' }],
                    include: {
                        post: { select: { id: true, sequence: true, title: true } },
                    },
                },
            },
        });

        expect(row?.posts).toHaveLength(2);
        expect(row?.comments).toHaveLength(2);
        expect(row?.posts[0]?.id).toBe('p2');
        expect(row?.posts[0]?.comments[0]?.author?.id).toBe('u1');
        expect(row?.comments[0]?.post?.id).toBeDefined();
    });

    it('supports post findMany with comments including author + post select (wide nested shape)', async () => {
        db = await createTestClient(schema);

        await db.user.create({ data: { id: 'u1', email: 'u1@example.com' } });
        await db.post.createMany({
            data: [
                { id: 'p1', sequence: 1, title: 'P1', authorId: 'u1' },
                { id: 'p2', sequence: 2, title: 'P2', authorId: 'u1' },
            ],
        });
        await db.comment.createMany({
            data: [
                { id: 'c1', postId: 'p1', content: 'a', authorId: 'u1' },
                { id: 'c2', postId: 'p1', content: 'b', authorId: null },
            ],
        });

        const posts = await db.post.findMany({
            where: { authorId: 'u1' },
            orderBy: [{ sequence: 'desc' }],
            include: {
                author: { select: { id: true, email: true } },
                comments: {
                    orderBy: [{ id: 'asc' }],
                    include: {
                        author: true,
                        post: { select: { id: true, title: true, sequence: true } },
                    },
                },
            },
        });

        expect(posts).toHaveLength(2);
        const p1 = posts.find((p: any) => p.id === 'p1');
        expect(p1?.comments).toHaveLength(2);
        expect(p1?.comments[0]?.post?.title).toBe('P1');
    });
});

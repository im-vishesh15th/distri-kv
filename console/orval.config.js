module.exports = {
  petstore: {
    input: {
      target: '../docs/openapi.yaml',
    },
    output: {
      target: 'src/generated/api.ts',
      client: 'react-query',
      mode: 'tags-split',
      tag: true,
      prettier: true,
      override: {
        mutator: {
          path: './src/lib/axios-instance.ts',
          name: 'customInstance',
        },
        query: {
          useQuery: true,
          useInfiniteQuery: false,
        },
        mutator: {
          path: './src/lib/axios-instance.ts',
          name: 'customInstance',
        },
      },
    },
    hooks: {
      afterAllFilesWrite: 'prettier --write',
    },
  },
}
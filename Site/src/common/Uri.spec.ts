import { uri } from './Uri'

test('part appended', () => {
  expect(uri('http://test.com/folder').addPath('file.json').url).toBe('http://test.com/folder/file.json'),
    expect(uri('http://test.com/folder/').addPath('file.json').url).toBe('http://test.com/folder/file.json'),
    expect(
      uri('https://test.com')
        .with({
          host: 'testing.com'
        })
        .addQuery({ filter: 'q' })
        .with({
          username: 'mark',
          password: 'pass',
          port: 7071,
          path: ['a', 'b']
        })
        .addQuery({ prop: 's' })
        .addPath('c')
        .url
    ).toBe('https://mark:pass@testing.com:7071/a/b/c?filter=q&prop=s')
})

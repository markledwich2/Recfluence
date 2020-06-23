
import _, { toNumber } from 'lodash'

interface UriParts {
  uri?: string
  protocol?: string
  host?: string
  port?: number
  username?: string
  password?: string
  path?: string[]
  query?: { [key: string]: string }
  hash?: string
}

export class Uri {
  parts: UriParts

  constructor(uri: string | UriParts) {
    this.parts = typeof uri === 'string' ? this.asUri(uri) : uri
  }

  asUri = (url: string): UriParts => {
    const p = new URL(url)

    const q: { [key: string]: string } = {}
    for (const [key, value] of p.searchParams.entries())
      q[key] = value

    return {
      protocol: p.protocol,
      host: p.host,
      port: p.port ? toNumber(p.port) : null,
      username: p.username,
      password: p.password,
      path: p.pathname?.split('/').filter(p => p),
      query: q,
      hash: p.hash
    }
  };

  asUrl = (p: UriParts): URL => {
    const u = new URL(`${p.protocol ?? '_:'}//${p.host ?? '_'}`)
    if (p.port) u.port = p.port.toString()
    if (p.username) u.username = p.username
    if (p.password) u.password = p.password
    if (p.path?.length > 0) u.pathname = p.path.join('/')
    if (p.hash) u.hash = p.hash
    if (p.query)
      for (const [key, value] of Object.entries(p.query))
        u.searchParams.append(key, value)
    return u
  };

  with = (parts: UriParts) => new Uri(Object.assign({}, this.parts, parts))

  /** appends the given query string values to the existing, returns the new Uri */
  addQuery = (q: { [key: string]: string }) => this.with({ query: Object.assign({}, this.parts.query, q) })

  /** appends a path part to the existing one. returns a new Uri */
  addPath = (...path: string[]) => this.with({ path: (this.parts.path ?? []).concat(path) })

  get url() { return this.asUrl(this.parts).toString() }
  public toString() { return this.url };
}

/** creates a Uri object for fluently building/modifying a url */
export function uri(uri: string) {
  return new Uri(uri)
}
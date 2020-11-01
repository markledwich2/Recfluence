// this is a workaround issue with v3 https://github.com/JedWatson/react-select/issues/3592
declare module 'react-select/async' {
  import Async from 'react-select/lib/Async'
  export * from 'react-select/lib/Async'
  export default Async
}

declare module 'react-select/async-creatable' {
  import AsyncCreatable from 'react-select/lib/AsyncCreatable'
  export * from 'react-select/lib/AsyncCreatable'
  export default AsyncCreatable
}
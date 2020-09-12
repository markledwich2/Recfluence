// this is a workaround issue with v3 https://github.com/JedWatson/react-select/issues/3592
declare module 'react-select/async' {
  import Async from 'react-select/lib/Async'
  export * from 'react-select/lib/Async'
  export default Async
}
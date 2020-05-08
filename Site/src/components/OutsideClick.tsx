import React, { MutableRefObject } from 'react'
import { useEffect } from "react"

const useOutsideClick = (ref: MutableRefObject<any>, callback: (e: Element) => void) => {
  const handleClick = (e: any) => {
    if (ref.current && !ref.current.contains(e.target)) callback(e.target)
  }

  useEffect(() => {
    document.addEventListener("click", handleClick)
    return () => document.removeEventListener("click", handleClick)
  })
}

export default useOutsideClick
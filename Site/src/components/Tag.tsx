
import React, { useState, PropsWithChildren, MouseEventHandler } from "react"
import styled from 'styled-components'

const TagStyle = styled.span`
  background-color: rgb(66, 66, 66);
  font-size: 0.9em;
  font-weight: bold;
  line-height: 3;
  border-radius: 5px;
  padding: 3px 6px;
  margin-right: 6px;
`

export const Tag = ({ color, label }: { color?: string, label: string }) => <TagStyle style={{ backgroundColor: color }}>{label}</TagStyle>
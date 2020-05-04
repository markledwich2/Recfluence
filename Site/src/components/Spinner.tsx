import React from 'react'
import styled from 'styled-components'
import { theme } from './MainLayout'

const SvgStyle = styled.svg`
    position:absolute;
    left:50%;
    transform:translate(-50%, 0);
    margin: auto; 
    background: none; 
    display: block; 
    shape-rendering: auto;
`

export const Spinner = ({ size }: { size: string }) => <SvgStyle width={size} height={size} viewBox="0 0 100 100" preserveAspectRatio="xMidYMid">
    <circle cx="38.8072" cy="50" fill={theme.backColorBolder} r="22">
        <animate attributeName="cx" repeatCount="indefinite" dur="0.7194244604316546s" keyTimes="0;0.5;1" values="28;72;28" begin="-0.3597122302158273s"></animate>
    </circle>
    <circle cx="61.1928" cy="50" fill={theme.backColorBolder2} r="22">
        <animate attributeName="cx" repeatCount="indefinite" dur="0.7194244604316546s" keyTimes="0;0.5;1" values="28;72;28" begin="0s"></animate>
    </circle>
    <circle cx="38.8072" cy="50" fill={theme.backColorBolder} r="22">
        <animate attributeName="cx" repeatCount="indefinite" dur="0.7194244604316546s" keyTimes="0;0.5;1" values="28;72;28" begin="-0.3597122302158273s"></animate>
        <animate attributeName="fill-opacity" values="0;0;1;1" calcMode="discrete" keyTimes="0;0.499;0.5;1" dur="0.7194244604316546s" repeatCount="indefinite"></animate>
    </circle>
</SvgStyle>
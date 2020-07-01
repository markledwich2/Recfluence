import React, { useState, PropsWithChildren, MouseEventHandler, CSSProperties } from "react"
import styled from 'styled-components'
import { ytTheme } from './MainLayout'
import { StyledIconBase } from '@styled-icons/styled-icon'

interface ButtonProps {
    label?: string
    icon?: JSX.Element
    onclick: MouseEventHandler<JSX.Element>
    primary?: boolean
}

const ButtonStyle = styled.button`
    display:flex;

    text-transform: uppercase;
    text-align: center;

    
    border: none;
    cursor: pointer;
    align-self: center;
    color: ${ytTheme.fontColor};
    
    background-color: ${p => p.primary ? ytTheme.themeColorSubtler : ytTheme.backColorBolder};
    :hover {
        background-color: ${ytTheme.backColorBolder3};
    }
    font-size: 1em;
    line-height: 1em;
    padding: .5em 1em 0.5em 1em;
    border-radius: 0.2em;
    outline: none;
    font-weight: bolder;

    ${StyledIconBase} {
        height: 1.4em;
        width: 1.4em;
        position: relative;
        top: -0.15em;
        padding-right: 0.2em;
    }
`

export const Button = ({ label, icon, onclick, primary }: ButtonProps) => <ButtonStyle onClick={onclick} primary={primary}>{icon} {label}</ButtonStyle>

export const inlineButtonStyle: CSSProperties = {
    height: '1.4em',
    width: '1.4em',
    position: 'relative',
    top: '-0.10em',
    paddingRight: '0.2em'
}
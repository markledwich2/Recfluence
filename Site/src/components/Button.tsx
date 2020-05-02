import React, { useState, PropsWithChildren, MouseEventHandler } from "react"
import styled from 'styled-components'
import { theme } from './MainLayout'
import { StyledIconBase } from '@styled-icons/styled-icon'

interface ButtonProps {
    label?: string
    icon?: JSX.Element
    onclick: MouseEventHandler<JSX.Element>
}

const ButtonStyle = styled.button`
    display:flex;

    text-transform: uppercase;
    text-align: center;

    background-color: ${theme.backColor1};
    border: none;
    cursor: pointer;
    align-self: center;
    color: ${theme.fontColor};

    font-size: 1em;
    line-height: 1em;
    padding: .5em 1em 0.2em 1em;
    margin: 0.7em 0.1em;
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

export const Button = ({ label, icon, onclick }: ButtonProps) => <ButtonStyle onClick={onclick}>{icon} {label}</ButtonStyle>
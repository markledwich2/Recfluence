import React, { useState, PropsWithChildren, MouseEventHandler } from "react"
import styled from 'styled-components'
import { theme } from './MainLayout'
import { StyledIconBase } from '@styled-icons/styled-icon'

interface ButtonProps {
    label: string
    icon?: JSX.Element
    onclick: MouseEventHandler<JSX.Element>
}

const ButtonStyle = styled.button`
    text-transform: uppercase;
    text-align: center;

    background-color: ${theme.backColor};
    border: none;
    cursor: pointer;
    align-self: center;
    color: ${theme.fontColor};

    font-size: 1.2em;
    line-height: 1.2em;
    padding: .3em .5em;
    border-radius: 0.5em;
    outline: none;
    font-weight: bolder;
    margin: 2px;

    ${StyledIconBase} {
        height: 1.4em;
        width: 1.4em;
        position: relative;
        top: -0.1em;
        padding-right: 0.2em;
    }
`

export const Button = ({ label, icon, onclick }: ButtonProps) => <ButtonStyle onClick={onclick}>{icon} {label}</ButtonStyle>
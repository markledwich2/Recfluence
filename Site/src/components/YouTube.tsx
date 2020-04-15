import React from "react"
import styled from 'styled-components'

interface YouTubeOptions {
    id: string
    start?: number
}

const ContainerDiv = styled.div`
    position:relative;
    padding-top:56.25%;
`



export const YouTube: React.FC<YouTubeOptions> = (props) => {
    return <ContainerDiv>
        <iframe width="100%" height="100%" src={`https://www.youtube.com/embed/${props.id}` + (props.start ? `start=${props.start}` : '')}
            allow="accelerometer; autoplay; encrypted-media; gyroscope; picture-in-picture" frameborder="0" allowfullscreen
            style={{ position: 'absolute', top: 0, left: 0 }} ></iframe>
    </ContainerDiv>
}
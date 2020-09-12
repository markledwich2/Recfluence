import { ChannelLogo } from '../channel/Channel'
import TimeAgo from 'javascript-time-ago'
import en from 'javascript-time-ago/locale/en'
import { BasicChannel, ChannelReview, Review } from '../../common/YtApi'
import styled from 'styled-components'
import * as React from "react"
import _ from 'lodash'
import { tagOptions, LrTag } from './ReviewCommon'
import { Tag } from '../Tag'
import { Button } from '../Button'
import { Edit as EditIcon } from '@styled-icons/material'

TimeAgo.addLocale(en)
const timeAgo = new TimeAgo('en')

const ReviewedTableStyle = styled.table`
  border-spacing: 1.5em;
  border-collapse: separate;

  th {
    text-align:left;
  }
  td {
    h4 {
      padding: 0.1em;
    }
  }
`

export const ReviewedGrid = ({ reviews, onEditReview }:
  {
    reviews: ChannelReview[],
    onEditReview: (c: ChannelReview) => void
  }) => {

  return <ReviewedTableStyle>
    <thead>
      <tr>
        <th></th>
        <th>L/R and Tags</th>
        <th>Notes</th>
        <th>Relevance</th>
      </tr>
    </thead>
    <tbody>
      {reviews &&
        reviews.map(cr => {
          var c = cr.channel
          var r = cr.review
          return <tr key={`${r.channelId}|${r.updated}`}>
            <td>
              <div style={{ display: 'flex', flexDirection: 'row' }}>
                <ChannelLogo channelId={r.channelId} thumb={c?.logoUrl} style={{ height: '50px', verticalAlign: 'middle', margin: '0 5px' }} />
                <div>
                  <h4>{c?.channelTitle ?? r.channelId}</h4>
                  <div>{timeAgo.format(Date.parse(r.updated))}</div>
                  {r.mainChannelId ? <div> Main: <b>{cr.mainChannel?.channelTitle ?? r.mainChannelId}</b></div> : null}
                </div>
              </div>
            </td>
            <td>
              <div style={{ margin: '0.3em' }} ><LrTag tag={r.lr} /></div>
              <div>
                {_.uniq(r.softTags)
                  .map(t => tagOptions.find(o => o.value == t))
                  .filter(t => t)
                  .map(o => <Tag key={o.value} label={o.label} style={{ margin: '0.3em' }} />)}
              </div>
            </td>
            <td style={{ maxWidth: '20em' }}>{r?.notes}</td>
            <td style={{ textAlign: 'right' }}>{r.relevance}</td>
            <td><Button onclick={_ => onEditReview(cr)} icon={<EditIcon />} /></td>
          </tr>
        }
        )}
    </tbody>
  </ReviewedTableStyle>
}

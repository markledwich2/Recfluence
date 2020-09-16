import { ChannelLogo } from '../channel/Channel'
import TimeAgo from 'javascript-time-ago'
import en from 'javascript-time-ago/locale/en'
import { BasicChannel, ChannelReview, Review, getChannels } from '../../common/YtApi'
import styled from 'styled-components'
import React, { useState, useContext, useEffect } from "react"
import _ from 'lodash'
import { tagOptions, LrTag, Field, tagCustomOption, Option, FlexRow } from './ReviewCommon'
import { Tag } from '../Tag'
import { Button, inlineButtonStyle } from '../Button'
import { Edit as EditIcon, Search as SearchIcon } from '@styled-icons/material'
import { EsContext } from '../SearchContext'
import Select from 'react-select'
import { selectStyle, selectTheme, ytTheme } from '../MainLayout'
import { assign } from '../../common/Utils'

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

interface Filter {
  text?: string,
  tags: string[]
}

export const ReviewedGrid = ({ reviews, onEditReview }:
  {
    reviews: ChannelReview[],
    onEditReview: (c: ChannelReview) => void
  }) => {

  const [reviewsPage, setReviewPage] = useState<number>(1)
  const [reviewsShown, setReviewsShown] = useState<{ reviews: ChannelReview[], more: boolean }>()
  const [filter, setFilter] = useState<Filter>({ tags: [] })
  const esCfg = useContext(EsContext)

  useEffect(() => {
    if (!reviews) return

    const textRe = filter.text ? new RegExp(`${filter.text}`, 'i') : null

    const toShow = reviewsPage * 50

    const go = async () => {
      const rShown = _(reviews)
        .groupBy(r => r.review.channelId).map(g => _(g).orderBy(r => r.review.updated, 'desc').head()) // remove dupes
        .filter(r => (!filter.text || textRe?.test(r.channel.channelTitle))
          && (!filter.tags || filter.tags.every(t => r.review.softTags.includes(t))))
        .orderBy(r => r.review.updated, 'desc').value() // take recent 50
      setReviewsShown({ reviews: _.slice(rShown, 0, toShow), more: rShown.length > toShow })
    }
    go()
  }, [reviews, reviewsPage, filter])


  return <>
    <div style={{ backgroundColor: ytTheme.backColorBolder, padding: '1em' }}>
      <FlexRow space='2em' >
        <b>Filter</b>
        <Field label='Title/Notes'>
          <input type='text'
            style={{ backgroundColor: ytTheme.backColor, width: '20em' }}
            onChange={e => setFilter(assign(filter, { text: e.target.value }))} />
        </Field>
        <Field label='Tags' style={{ minWidth: '400px' }}>
          <Select
            id='SoftTags'
            isMulti
            value={tagOptions.filter(o => filter.tags?.find(t => t == o.value))}
            options={tagOptions}
            onChange={(options: Option[]) => {
              const newFilter = assign(filter, { tags: options?.map(o => o.value) ?? [] })
              return setFilter(newFilter)
            }}
            styles={selectStyle} theme={selectTheme} />
        </Field>
      </FlexRow>
    </div>


    <ReviewedTableStyle>
      <thead>
        <tr>
          <th></th>
          <th>L/R and Tags</th>
          <th>Notes</th>
          <th>Relevance</th>
        </tr>
      </thead>
      <tbody>
        {reviewsShown &&
          reviewsShown.reviews.map(cr => {
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
    {reviewsShown?.more && <div>
      <a onClick={() => setReviewPage(reviewsPage + 1)}>show more reviews</a>
    </div>}
  </>
}

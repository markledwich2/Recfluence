
import * as React from "react"
import { useEffect, useState, useRef, FunctionComponent, useMemo, useContext } from 'react'
import { Review, BasicChannel, ChannelReview, channelSearch, getChannel } from '../../common/YtApi'
import { ytTheme, selectStyle, selectTheme } from '../MainLayout'
import { ChannelLogo, channelUrl } from '../channel/Channel'
import styled from 'styled-components'
import Select from 'react-select'
import _ from 'lodash'
import { Option, lrOptions, LrCustomLabel, tagCustomOption, tagOptions, Field, FlexRow, FlexCol, FormStyle, loadChannelOptions } from './ReviewCommon'
import ReactTooltip from 'react-tooltip'
import ReactMarkdown from 'react-markdown'
import Async, { makeAsyncSelect } from 'react-select/async'
import { EsContext } from '../SearchContext'

const Md = styled(ReactMarkdown)`
  max-width:30em;

  p {
    margin: 0.5em 0 0.8em 0
  }

  ul {
    padding-left: 1em
  }
`

const tips = {
  lr: `**For news:**
- Compare https://www.adfontesmedia.com and https://mediabiasfactcheck.com/. If they exist in those lists and were in agreement then I accepted that category. Otherwise I used the same process as with the commentary channels.
  

**For political/cultural commentary** consider:
  - Self identified political label, or support for a party
  - One sided content on divided political topics of the day (e.g. Kavanaugh, Migrant Caravan)
  - One sided content reacting to cultural events topics of the day (e.g. campus protests, trans activism )
  - Clearly matches Democrat (left) or Republican (right) views on issues in the [ISideWith poll](https://www.isidewith.com/en-us/polls)
  
  If these considerations align in the same direction then the channel is left or right. If there was a mix then they are assigned the center/heterodox category.
`,
  tag: `Choose all tags that apply. Help for each tag within the drop-down control.`,
  mainChannel: 'The parent channel (if applicable). \n\n Example: `Fox 10 Phoenix` should have this set as `Fox News`',
  relevance: 'The portion of content relevant to politics, cultural commentary or morally important sense-making. English speaking only. ',
  notes: 'Relevant notes about this channel. This is useful if we need to go back and ad new tags, or diagnose disagreements',
  save: '**Save** `crl+s`',
  nonPolitical: '**Save as non-political** `crl+n`'
}

export const ReviewForm = ({ review, onSave, onSaveNonPolitical, onChange, onCancel, onSkip, reviewValid }: {
  review: ChannelReview,
  onSave: (r: ChannelReview) => Promise<void>,
  onSaveNonPolitical: (r: ChannelReview) => Promise<void>,
  onChange: (r: ChannelReview) => void,
  onCancel?: () => void,
  onSkip?: () => void,
  reviewValid: (r: Review) => boolean,
}) => {

  const lrRef = useRef(null)
  const channelId = review?.review?.channelId
  const [prevReviewId, setPrevReviewId] = useState<string>(null)
  const [mainChannel, setMainChannel] = useState<BasicChannel>(null)
  const isNewReview = channelId != prevReviewId


  useEffect(() => { if (isNewReview) lrRef.current.focus() }, [isNewReview])
  if (isNewReview) setPrevReviewId(channelId)

  useEffect(() => {
    if (!review?.review?.mainChannelId) return
    getChannel(esCfg, review.review.mainChannelId).then(c => setMainChannel(c))
  }, [review])

  if (!review) return <></>

  const esCfg = useContext(EsContext)
  const c = review.channel
  const r = review.review

  const updateReviewProp = (p: keyof Review, v: any) => onChange({ ...review, review: { ...r, [p]: v } })

  //const { channelOptions, channelDic } = createChannelOptions(channels)

  return <>{c && r && (<>
    <FlexRow>
      <div>
        <ChannelLogo channelId={c.channelId} thumb={c.logoUrl} style={{ margin: '10px 5px', width: '70px' }} />
      </div>
      <FlexCol>
        <div>
          <h2>{c?.channelTitle ?? "No channel to review"}</h2>
        </div>
        <FlexRow space='2em'>
          <div><a href={channelUrl(c.channelId)} target="_new">YouTube Channel</a></div>
          <div><a href={`/search?channel=%5B"${c.channelTitle}"%5D&part=%5B"Title"%5D`} target="_new">Recfluence Search</a></div>
        </FlexRow>
        <span>{c.description}</span>
      </FlexCol>
    </FlexRow>
  </>)
  }

    <form onSubmit={() => {
      event.preventDefault()
      onSave(review)
    }}>
      <FormStyle space='1.5em'>
        <Field name='lr' label='Left/Center/Right' size='l' >
          <Select
            id='LR'
            ref={lrRef}
            value={r.lr ? lrOptions.find(o => o.value == r.lr) : null}
            onChange={(o: Option) => updateReviewProp('lr', o.value)}
            formatOptionLabel={LrCustomLabel}
            options={lrOptions}
            styles={selectStyle} theme={selectTheme} autoFocus required />
        </Field>

        <Field name='tag' label='Tag(s)' size='l' >
          <Select
            id='SoftTags'
            isMulti
            components={{ Option: tagCustomOption }}
            value={tagOptions.filter(o => r.softTags?.find(t => t == o.value))}
            options={tagOptions}
            onChange={(options: Option[]) => updateReviewProp('softTags', options?.map(o => o.value) ?? [])}
            styles={selectStyle} theme={selectTheme} />
        </Field>

        <Field name='mainChannel' label='Main (Parent) Channel' size='l' >
          <Async
            id='MainChannelId'
            value={r.mainChannelId ? { value: r.mainChannelId, label: mainChannel?.channelTitle ?? r.mainChannelId } : null}
            isClearable
            backspaceRemovesValue
            loadOptions={s => loadChannelOptions(esCfg, s)}
            onChange={(o: Option) => updateReviewProp('mainChannelId', o?.value)}
            styles={selectStyle} theme={selectTheme}
          />
        </Field>

        <Field name='relevance' label='Relevance' required >
          <input id='Relevance'
            value={r?.relevance ?? ''}
            onChange={e => updateReviewProp('relevance', e.target.value)}
            type='number'
            placeholder='Relevance/10'
            min={0} max={10}
            style={{ width: '10em' }} />
        </Field>

        <Field name='notes' label='Notes' size='l' >
          <textarea id='Notes' value={r?.notes ?? ''} onChange={e => updateReviewProp('notes', e.target.value)} placeholder='Notes...' rows={3} style={{ width: '100%' }}></textarea>
        </Field>

        <div>
          <input type='submit' value='save' disabled={!reviewValid(r)} name='save' data-tip='save' />
          <input type='submit' value='not political' onClick={e => {
            e.preventDefault()
            onSaveNonPolitical(review)
          }} data-tip='nonPolitical' />
          {onSkip && <button onClick={onSkip} type='button'>Skip</button>}
          {onCancel && <button onClick={onCancel} type='button'>Cancel</button>}
        </div>

        <ReactTooltip
          place='right' effect='solid'
          backgroundColor={ytTheme.backColor}
          border
          borderColor={ytTheme.backColorBolder2}
          getContent={(e: keyof typeof tips) => <Md source={tips[e] ?? e} />} />
      </FormStyle>
    </form>
  </>
}




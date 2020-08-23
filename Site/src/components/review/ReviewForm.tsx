
import * as React from "react"
import { useEffect, useState, useRef, FunctionComponent, useMemo } from 'react'
import { Review, BasicChannel } from '../../common/YtApi'
import { ytTheme, selectStyle, selectTheme } from '../MainLayout'
import { ChannelLogo, channelUrl } from '../channel/Channel'
import styled from 'styled-components'
import Select from 'react-select'
import _ from 'lodash'
import { useHotkeys, Options } from 'react-hotkeys-hook'
import { ChannelReview, Option, lrOptions, LrCustomLabel, tagCustomOption, tagOptions, createChannelOptions } from './ReviewCommon'
import ReactTooltip from 'react-tooltip'
import ReactMarkdown from 'react-markdown'
import { HelpOutline } from '@styled-icons/material'
import { inlineButtonStyle } from '../Button'

const FlexRow = styled.div<{ space?: string }>`
  display:flex;
  flex-direction: row;
  > * {
    padding-right: ${p => p.space ?? '0.6em'};
  }
`

const FlexCol = styled.div<{ space?: string }>`
  display:flex;
  flex-direction: column;
  > * {
    padding-bottom: ${p => p.space ?? '0.6em'};
  }
`

const FormStyle = styled(FlexCol)`
  label {
    line-height:2em;
    color:${ytTheme.fontColorSubtler}
  }

  input[type=submit] {
    margin-right:1em;
  }
`

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

const Help = (p: { name: keyof typeof tips }) => <HelpOutline
  color={ytTheme.backColorBolder3}
  style={{ ...inlineButtonStyle, marginLeft: '0.5em' }}
  data-tip={p.name}
  {...p} />

const fieldSizes = {
  's': '10em',
  'l': '50em'
}

interface FieldProps {
  name: keyof typeof tips
  size?: keyof typeof fieldSizes
  label: string
  required?: boolean
}

const Field: FunctionComponent<FieldProps> = ({ name, size, label, children, required }) =>
  <div style={{ maxWidth: fieldSizes[size ?? 's'] }}>
    <label>{label} {required && <Mandatory />} <Help name={name} />
      {children}
    </label>
  </div>

export const ReviewForm = ({ review, onSave, onSaveNonPolitical, onChange, onCancel, reviewValid, channels }: {
  review: ChannelReview,
  onSave: (r: ChannelReview) => Promise<void>,
  onSaveNonPolitical: (r: ChannelReview) => Promise<void>,
  onChange: (r: ChannelReview) => void,
  onCancel?: () => void,
  reviewValid: (r: Review) => boolean,
  channels: _.Dictionary<BasicChannel>
}) => {

  const lrRef = useRef(null)
  const channelId = review?.review.ChannelId
  const [prevReviewId, setPrevReviewId] = useState<string>(null)
  const isNewReview = channelId != prevReviewId
  useEffect(() => { if (isNewReview) lrRef.current.focus() }, [isNewReview])
  if (isNewReview) setPrevReviewId(channelId)

  if (!review) return <></>

  const c = review.channel
  const r = review.review

  const updateReviewProp = (p: keyof Review, v: any) => onChange({ ...review, review: { ...r, [p]: v } })

  const { channelOptions, channelDic } = createChannelOptions(channels)

  return <>{c && r && (<>
    <FlexRow>
      <div>
        <ChannelLogo channelId={c.ChannelId} thumb={c.LogoUrl} style={{ margin: '10px 5px' }} />
      </div>
      <FlexCol>
        <div>
          <h2>{c?.ChannelTitle ?? "No channel to review"}</h2>
        </div>
        <FlexRow space='2em'>
          <div><a href={channelUrl(c.ChannelId)} target="_new">YouTube Channel</a></div>
          <div><a href={`/search?channel=%5B"${c.ChannelTitle}"%5D&part=%5B"Title"%5D`} target="_new">Recfluence Search</a></div>
        </FlexRow>
        <span>{c.Description}</span>
      </FlexCol>
    </FlexRow>
  </>)
  }

    <form onSubmit={() => {
      event.preventDefault()
      onSave(review)
    }}>
      <FormStyle space='1.5em'>
        <Field name='lr' label='Left/Center/Right' >
          <Select
            id='LR'
            ref={lrRef}
            value={r.LR ? lrOptions.find(o => o.value == r.LR) : null}
            onChange={(o: Option) => updateReviewProp('LR', o.value)}
            formatOptionLabel={LrCustomLabel}
            options={lrOptions}
            styles={selectStyle} theme={selectTheme} autoFocus required />
        </Field>

        <Field name='tag' label='Tag(s)' size='l' >
          <Select
            id='SoftTags'
            isMulti
            components={{ Option: tagCustomOption }}
            value={tagOptions.filter(o => r.SoftTags?.find(t => t == o.value))}
            options={tagOptions}
            onChange={(options: Option[]) => updateReviewProp('SoftTags', options?.map(o => o.value) ?? [])}
            styles={selectStyle} theme={selectTheme} />
        </Field>

        <Field name='mainChannel' label='Main (Parent) Channel' size='l' >
          <Select
            id='MainChannelId'
            value={r.MainChannelId ? channelDic[r.MainChannelId] : null}
            isClearable
            backspaceRemovesValue
            options={channelOptions}
            onChange={(o: Option) => updateReviewProp('MainChannelId', o?.value)}
            styles={selectStyle} theme={selectTheme} />
        </Field>

        <Field name='relevance' label='Relevance' required >
          <input id='Relevance'
            value={r?.Relevance ?? ''}
            onChange={e => updateReviewProp('Relevance', e.target.value)}
            type='number'
            placeholder='Relevance/10'
            min={0} max={10}
            style={{ width: '10em' }} />
        </Field>

        <Field name='notes' label='Notes' size='l' >
          <textarea id='Notes' value={r?.Notes ?? ''} onChange={e => updateReviewProp('Notes', e.target.value)} placeholder='Notes...' rows={3} style={{ width: '100%' }}></textarea>
        </Field>

        <div>
          <input type='submit' value='save' disabled={!reviewValid(r)} name='save' data-tip='save' />
          <input type='submit' value='not political' onClick={e => {
            e.preventDefault()
            onSaveNonPolitical(review)
          }} data-tip='nonPolitical' />
          {onCancel && <button onClick={onCancel} >Cancel</button>}
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

const Mandatory = () => <span data-tip="required" aria-label="required">*</span>


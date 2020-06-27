import * as React from "react"
import { useContext, useEffect, useState } from 'react'
import { UserContext } from '../UserContext'
import { ChannelReviewAndData, channelsReviewed, ChannelReview, saveReview, RawChannelData, getChannels } from '../../common/YtApi'
import { Spinner } from '../Spinner'
import { ytTheme, selectStyle, selectTheme, ContentPageDiv } from '../MainLayout'
import { ChannelLogo, channelUrl } from '../channel/Channel'
import styled from 'styled-components'
import Select from 'react-select'
import _ from 'lodash'
import { ChannelTags, tagColor } from '../channel_relations/ChannelTags'
import { YtModel, ChannelData } from '../../common/YtModel'
import { Dim, ColEx } from '../../common/Dim'
import { Tag } from '../Tag'
import { Edit as EditIcon } from '@styled-icons/material'
import { Button } from '../Button'
import { jsonClone, dateFormat } from '../../common/Utils'
import TimeAgo from 'javascript-time-ago'
import en from 'javascript-time-ago/locale/en'
import ReactMarkdown from 'react-markdown'
import { OptionProps } from 'react-select/lib/components/Option'
import { ToastProvider, useToasts } from 'react-toast-notifications'


interface ReviewState {
  channels: _.Dictionary<RawChannelData>
  pending?: RawChannelData[]
  reviewing?: RawChannelData
  review?: ChannelReview
  reviewed?: ChannelReviewAndData[]
}
const reviewValid = (r: ChannelReview) => r.Relevance && r.LR
const makeReviewAndData = (r: ChannelReview, channels: _.Dictionary<RawChannelData>): ChannelReviewAndData =>
  ({ ...r, ...channels[r.ChannelId], ReviewUpdated: r.Updated })


const ReviewPageDiv = styled(ContentPageDiv)`
  padding: 2em;
  h1, h2, h3, h4 {
    padding: 0.8em 0 0.4em 0;
  }
  abbr {
    text-decoration: none;
  }
  abbr:hover::after {
    content: attr(data-title);
    position:relative;
    background-color: ${ytTheme.backColorBolder2};
    border-radius: 5px;
    left:1em;
    padding:0.4em;
}
`
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

const FormDiv = styled(FlexCol)`
  label {
    line-height:2em;
    color:${ytTheme.fontColorSubtler}
  }
`

export const ChannelReviewForm = () => {
  const userCtx = useContext(UserContext)
  const [review, setReview] = useState<ReviewState>(null)
  const { addToast } = useToasts()

  useEffect(() => {
    const go = async () => {
      const email = userCtx?.user?.email
      if (!email) return
      try {
        await init(email)
      } catch (e) {
        addToast(`unable to load reviews: ${e}`, { appearance: 'warning', autoDismiss: false })
      }
    }
    go()
  }, [userCtx])

  const init = async (email: string) => {
    const channelsTask = getChannels()
    const reviewedTask = channelsReviewed(email)
    const channels = _.keyBy(await channelsTask, c => c.ChannelId)
    const reviewed = _(await reviewedTask)
      .map(r => (makeReviewAndData(r, channels))).value()
    const reviewedDic = _.keyBy(reviewed, c => c.ChannelId)
    const s = reviewNextPending({
      channels: channels,
      pending: _.filter(channels, c => c.ReviewStatus == 'Pending' && !reviewedDic[c.ChannelId]),
      reviewed: reviewed
    })
    setReview(s)
  }

  const reviewNextPending = (s: ReviewState): ReviewState => {
    const c = s.pending.length > 0 ? s.pending[0] : null
    return {
      ...s,
      reviewing: c,
      review: c ? { ChannelId: c.ChannelId, SoftTags: [] } : null
    }
  }

  const reviewChannel = (s: ReviewState, c: ChannelReviewAndData): ReviewState => {
    c = jsonClone(c) // don't modify this
    var channel = s.channels[c.ChannelId]
    return {
      ...s,
      reviewing: channel,
      review: c ? { ChannelId: c.ChannelId, SoftTags: c.SoftTags, LR: c.LR, MainChannelId: c.MainChannelId, Relevance: c.Relevance, Notes: c.Notes } : null
    }
  }

  const onSubmit = async (d: ChannelReview) => {

    const c = review.channels[d.ChannelId]
    d = { ...d, Updated: new Date().toISOString(), Email: userCtx.user?.email }
    const res = await saveReview(d)

    res.ok ? addToast(`Saved channel :  ${c.ChannelTitle}`, { appearance: 'success', autoDismiss: true })
      : addToast(`Couldn't save:  ${await res.text()}`, { appearance: 'warning', autoDismiss: true })

    if (!res.ok) return

    const r = makeReviewAndData(d, review.channels)

    const pending = review.pending.filter(p => p.ChannelId != d.ChannelId)
    const s = reviewNextPending({
      ...review,
      reviewed: review.reviewed.concat(r),
      pending: pending
    })
    setReview(s)
    document.querySelector<HTMLDivElement>('#LR')?.focus()
  }

  const updateReview = (r: ChannelReview) => setReview({ ...review, review: r })
  const updateReviewProp = (p: keyof ChannelReview, v: any) => updateReview({ ...review.review, [p]: v })

  if (!userCtx?.user) return <span>Please log in to review channels</span>
  if (!review) return <><Spinner size='50px' /></>

  let c = review.reviewing
  let r = review.review
  const mainChannelOptions = _(review.channels).map(c => ({ label: c.ChannelTitle, value: c.ChannelId }))
    .keyBy(c => c.value).value()

  // fetch some channels for review & list existing
  return <ReviewPageDiv>
    {review.pending && (<div>
      <h3>To Review ({review.pending.length})</h3>
      <span>You're up to date. You hard worker you!</span>
    </div>)}

    {c && r && (<>
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

      <form onSubmit={e => {
        event.preventDefault()
        onSubmit(r)
      }}>
        <FormDiv space='1.5em'>
          <div style={{ width: '10em' }}>
            <label>Left/Center/Right <Mandatory />
              <Select
                id='LR'
                value={r.LR ? lrOptions.find(o => o.value == r.LR) : null}
                onChange={(o: Option) => updateReviewProp('LR', o.value)}
                formatOptionLabel={LrCustomLabel}
                options={lrOptions}
                styles={selectStyle} theme={selectTheme} autoFocus required />
            </label>

          </div>
          <div style={{ maxWidth: '50em' }}>
            <label>Tag(s)
            <Select
                id='SoftTags'
                isMulti
                components={{ Option: tagCustomOption }}
                value={tagOptions.filter(o => r.SoftTags?.find(t => t == o.value))}
                options={tagOptions}
                onChange={(options: Option[]) => updateReviewProp('SoftTags', options?.map(o => o.value) ?? [])}
                styles={selectStyle} theme={selectTheme} />
            </label>
          </div>
          <div style={{ maxWidth: '50em' }}>
            <label>Main (Parent) ChannelTags
            <Select
                id='MainChannelId'
                value={mainChannelOptions[r.MainChannelId]}
                isClearable
                backspaceRemovesValue
                options={_.values(mainChannelOptions)}
                onChange={(o: Option) => updateReviewProp('MainChannelId', o?.value)}
                styles={selectStyle} theme={selectTheme} />
            </label>
          </div>
          <div>
            <label>Relevance <Mandatory /> <br />
              <input id='Relevance'
                value={r.Relevance}
                onChange={e => updateReviewProp('Relevance', e.target.value)}
                type='number'
                placeholder='Relevance/10'
                required min={0} max={10}
                style={{ width: '10em' }} />
            </label>
          </div>
          <div style={{ maxWidth: '50em' }}><label>Notes
            <textarea id='Notes' value={r.Notes} onChange={e => updateReviewProp('Notes', e.target.value)} placeholder='Notes...' rows={3} style={{ width: '100%' }}></textarea>
          </label>
          </div>
          <div><input type='submit' value='SAVE' disabled={!reviewValid(r)} /></div>
        </FormDiv>
      </form>
    </>)
    }

    <h3>Reviewed</h3>
    <div>{review?.reviewed?.length <= 0 ?
      <>You haven't reviewed anything yet</> :
      <ReviewedGrid reviewed={review.reviewed} onEditReview={c => setReview(reviewChannel(review, c))} channels={review.channels} />}</div>
  </ReviewPageDiv >
}


const Mandatory = () => <abbr data-title="This field is mandatory" aria-label="required">*</abbr>

//#region Tags

const chanDim = new Dim<ChannelData>(YtModel.channelDimStatic.meta)
const tagCol = chanDim.col('tags')
const tagFunc = { label: (t: string) => YtModel.tagAlias[t] ?? _.startCase(t), color: ColEx.colorFunc(tagCol) }

interface Option {
  value?: string
  label?: string
}

interface TagOption extends Option {
  md?: string
}

const tagOptions: TagOption[] = _([
  { value: 'Conspiracy', md: 'Regularly promotes a variety of conspiracy theories. A conspiracy theory explains an evert/circumstance as the result of a secret plot that is not widely accepted to be true (even though sometimes it is). Example conspiracy theories: [Moon landings were faked](https://en.wikipedia.org/wiki/Moon_landing_conspiracy_theories), [QAnon](https://en.wikipedia.org/wiki/QAnon) & [Pizzagate](https://en.wikipedia.org/wiki/Pizzagate_conspiracy_theory), [Epstein was murdered](https://en.wikipedia.org/wiki/Death_of_Jeffrey_Epstein), [Trump-russia collusion](https://rationalwiki.org/wiki/Trump-Russia_connection). Examples: [X22Report](https://www.youtube.com/user/X22Report), [The Next News Network](https://www.youtube.com/user/NextNewsNetwork)' },
  { value: 'QAnon', md: 'A channel focused on [Q-Anon](https://en.wikipedia.org/wiki/QAnon). Q is a handle of someone with access to the "deep state" leaking plots against Trump and his supporters. Examples: [Edge of Wonder](https://www.youtube.com/channel/UCxC2RlwWGHnwXanvHNBmw2w), [prayingmedic](https://www.youtube.com/channel/UCSio3E7kYvPeHKhfuYZWriA)' },
  { value: 'Libertarian', md: 'A [political philosophy](https://en.wikipedia.org/wiki/Libertarianism) wth individual liberty as its main principal. Generally skeptical of authority and state power (e.g. regulation, taxes, government programs). Favor free markets and private ownership. To tag, this should be the main driver of their politics. Does not include libertarian socialists who also are anti-state but are anti-capitalist and promote communal living. Examples: [Reason](https://www.youtube.com/user/ReasonTV), [John Stossel](https://www.youtube.com/user/ReasonTV), [The Cato Institute](https://www.youtube.com/user/catoinstitutevideo)' },
  { value: 'AntiSJW', md: 'Significant focus on criticizing *Social Justice* (see below) with a positive view of the marketplace of ideas and discussing controversial topics. To tag, this should be a common focus in their content.|[MILO](https://www.youtube.com/user/yiannopoulosm), [Tim Pool](https://www.youtube.com/user/Timcasts)' },
  { value: 'SocialJustice', md: ' Beleive or promote: Identity Politics & Intersectionality (narratives of oppression though the combination of historically oppressed identities), *Political Correctness* (the restriction of ideas and words you can say in polite society), *Social Constructionism* (the idea that the differences between individuals and groups are explained entirely by environment. For example sex differences are caused by culture not by biological sex).<br><br>Content in reaction to Anti-SJW or conservative content.<br><br>Their supporters are active on [r/Breadtube](https://www.reddit.com/r/BreadTube/) and the creators often identify with this label. This tag only includes breadtuber’s if their content is criticizing ant-SJW’s (promoting socialism is its own, separate tag). Examples: [Peter Coffin](https://www.youtube.com/user/petercoffin), [hbomberguy](https://www.youtube.com/user/hbomberguy)' },
  { value: 'WhiteIdentitarian', md: 'Identifies-with/is-proud-of the superiority of “whites” and western Civilization.<br><br>An example of identifying with “western heritage”  would be to refer to the sistine chapel, or bach as “our culture”.<br><br>Promotes or defends: An ethno-state where residence or citizenship would be limited to “whites” OR a type of nationalist that seek to maintain a white national identity (white nationalism), historical narratives focused on the “white” lineage and its superiority, Essentialist concepts of racial differences<br><br>Are concerned about whites becoming a minority population in the US. Examples: [NPI / RADIX](https://www.youtube.com/user/NPIAmerica), [Stefan Molyneux](https://www.youtube.com/user/stefbot)' },
  { value: 'Educational', md: 'Channel that has significant focuses on education material related to politics/culture. Examples: [TED](https://www.youtube.com/user/TEDtalksDirector/videos), [SoulPancake](https://www.youtube.com/user/soulpancake)' },
  { value: 'LateNightTalkShow', md: 'Channel with content presented humorous monologues about the day\'s news, guest interviews and comedy sketches. Examples: [Last Week Tonight](https://www.youtube.com/user/LastWeekTonight), [Trevor Noah](https://www.youtube.com/channel/UCwWhs_6x42TyRM4Wstoq8HA)' },
  { value: 'PartisanLeft', md: 'Mainly focused on politics and exclusively critical of Republicans. Would agree with this statement: “GOP policies are a threat to the well-being of the country“. Examples: [The Young Turks](https://www.youtube.com/user/TheYoungTurks), [CNN](https://www.youtube.com/user/CNN)' },
  { value: 'PartisanRight', md: ' Mainly focused on politics and exclusively critical of Democrats. Would agree with this statement: “Democratic policies threaten the nation”. Examples: [Fox News](https://www.youtube.com/user/FoxNewsChannel),[Candace Owens](https://www.youtube.com/channel/UCL0u5uz7KZ9q-pe-VC8TY-w)' },
  { value: 'AntiTheist', md: 'Self-identified atheist who are also actively critical of religion. Also called New Atheists or Street Epistemologists. Usually combined with an interest in philosophy. Examples:[Sam Harris](https://www.youtube.com/user/samharrisorg), [CosmicSkeptic](https://www.youtube.com/user/alexjoconnor), [Matt Dillahunty](https://www.youtube.com/user/SansDeity)' },
  { value: 'ReligiousConservative', md: 'A channel with a focus on promoting Christianity or Judaism in the context of politics and culture. Examples: [Ben Shapiro](https://www.youtube.com/channel/UCnQC_G5Xsjhp9fEJKuIcrSw), [PragerU](https://www.youtube.com/user/PragerUniversity)' },
  { value: 'Socialist', md: 'Focus on the problems of capitalism. Endorse the view that capitalism is the source of most problems in society. Critiques of aspects of capitalism that are more specific (i.e. promotion of fee healthcare or a large welfare system or public housing) don’t qualify for this tag. Promotes alternatives to capitalism. Usually some form of either  Social Anarchist  (stateless egalitarian communities) or Marxist (nationalized production and a way of viewing society though class relations and social conflict). Examples: [BadMouseProductions](https://www.youtube.com/user/xaxie1), [NonCompete](https://www.youtube.com/channel/UCkZFKKK-0YB0FvwoS8P7nHg/videos)' },
  { value: 'Provocateur', md: 'Enjoys offending and receiving any kind of attention (positive or negative). Takes extreme positions, or frequently breaks cultural taboos. Often it is unclear if they are joking or serious. Examples:[StevenCrowder](https://www.youtube.com/user/StevenCrowder), [MILO](https://www.youtube.com/user/yiannopoulosm)' },
  { value: 'MRA', md: '(Men’s Rights Activist): Focus on advocating for rights for men. See men as the oppressed sex and will focus on examples where men are currently. Examples: [Karen Straughan](https://www.youtube.com/user/girlwriteswhat)' },
  { value: 'MissingLinkMedia', md: 'Channels funded by companies or venture capital, but not large enough to be considered “mainstream”. They are generally accepted as more credible than independent YouTube content. Examples: [Vox](https://www.youtube.com/user/voxdotcom) [NowThis News](https://www.youtube.com/user/nowthismedia)' },
  { value: 'StateFunded', md: 'Channels largely funded by a government. Examples:[PBS NewsHour](https://www.youtube.com/user/PBSNewsHour), [Al Jazeera](https://www.youtube.com/user/AlJazeeraEnglish), [RT](https://www.youtube.com/user/RussiaToday)' },
  { value: 'Mainstream News', md: 'Media institutions from TV, Cable or Newspaper that are also creating content for YouTube. Examples: [CNN](https://www.youtube.com/user/CNN), [Fox](https://www.youtube.com/user/FoxNewsChannel), [NYT](https://www.youtube.com/user/TheNewYorkTimes)' },
  { value: 'Politician', md: 'The channel is on behalf of a currently running/in-office Politician. Examples [Alexandria Ocasio-Cortez](https://www.youtube.com/channel/UCElqfal0wzzpLsHlRuqZjaA), [Donald J Trump](https://www.youtube.com/channel/UCAql2DyGU2un1Ei2nMYsqOA)' },
  { value: 'Black', md: 'African American creators focused on cultural/political issues of their community/identity (e.g. Police Violence, Racism). Examples:[African Diaspora News Channel](https://www.youtube.com/channel/UCKZGcrxRAhdUi58Mdr565mw), [Roland S. Martin](https://www.youtube.com/channel/UCjXB7nX8bL2U2sje8d212Yw), [Lisa Cabrera](https://www.youtube.com/channel/UCcTzK_2JDmFYGnUiaUleQYg) ' },
  { value: 'LGBT', md: 'LGBT creators focused on cultural/political issues of their community/identity (e.g. gender and sexuality, trans experiences). Examples: [ContraPoints](https://www.youtube.com/user/ContraPoints), [Kat Blaque](https://www.youtube.com/channel/UCxFWzKZa74SyAqpJyVlG5Ew)' }
]).map(t => ({ ...t, label: tagFunc.label(t.value) })).orderBy(t => t.label).value()

const TagDiv = styled.div`
  padding: 0.5em;
  cursor: pointer;
  .head, .head label  {
    cursor: pointer;
  }

  label {
    font-weight:bolder;
  }

  &.focused {
    background-color: ${ytTheme.backColorBolder3}
  }

  &.selected {
    background-color: ${ytTheme.backColorBolder2}
  }
`
const CustomARender = (props: any) => <a {...props} onClick={e => e.stopPropagation()} target='_new' />

const tagCustomOption = ({ innerRef, innerProps, isDisabled, data, isFocused, isSelected }: OptionProps<TagOption>) =>
  <TagDiv ref={innerRef} {...innerProps} className={[isSelected ? 'selected' : null, isFocused ? 'focused' : null].filter(n => n).join(' ')}>
    <div className='head'><label>{data.label}</label></div>
    <div className='md' style={{ padding: '0 1em' }}>
      <ReactMarkdown source={data.md} escapeHtml={false} renderers={{ link: CustomARender }} />
    </div>
  </TagDiv>


const lrCol = chanDim.col('lr')
const lrFunc = { label: ColEx.labelFunc(lrCol), color: ColEx.colorFunc(lrCol) }
const lrOptions = lrCol.values.filter(v => v.value)

const LrTag = ({ tag }: { tag: string }) => <Tag key={tag} label={lrFunc.label(tag)} color={tagColor(lrFunc.color(tag))} />
const LrCustomLabel = ({ value }: any) => <LrTag tag={value} />

//#endregion



//#region Reviewed Grid

TimeAgo.addLocale(en)
const timeAgo = new TimeAgo('en')

const ReviewedTable = styled.table`
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

const ReviewedGrid = ({ reviewed, onEditReview, channels }:
  { reviewed: ChannelReviewAndData[], onEditReview: (c: ChannelReviewAndData) => void, channels: _.Dictionary<RawChannelData> }) => {
  let reviewedList = _(reviewed)
    .groupBy(r => r.ChannelId)
    .mapValues(g => _.orderBy(g, r => r.ReviewUpdated, 'desc')[0])
    .orderBy(r => r.ReviewUpdated, 'desc').value()

  return <ReviewedTable>
    <thead>
      <tr>
        <th></th>
        <th>L/R and Tags</th>
        <th>Relevance</th>
      </tr>
    </thead>
    <tbody>
      {reviewedList.map(c =>
        <tr key={`${c.ChannelId}|${c.ReviewUpdated}`}>

          <td>
            <div style={{ display: 'flex', flexDirection: 'row' }}>
              <ChannelLogo channelId={c.ChannelId} thumb={c.LogoUrl} style={{ height: '50px', verticalAlign: 'middle', margin: '0 5px' }} />
              <div>
                <h4>{c.ChannelTitle ?? c.ChannelId}</h4>
                <div>{timeAgo.format(Date.parse(c.ReviewUpdated))}</div>
                {c.MainChannelId ? <div> Main: <b>{channels[c.MainChannelId]?.ChannelTitle ?? c.MainChannelId}</b></div> : null}
              </div>
            </div>
          </td>
          <td>
            <div><LrTag tag={c.LR} /></div>
            <div>
              {c.SoftTags.map(t => <Tag key={t} label={tagFunc.label(t)} color={tagColor(tagFunc.color(t))} style={{ margin: '0 0.5em 0 0' }} />)}
            </div>
          </td>
          <td style={{ textAlign: 'right' }}>{c.Relevance}</td>
          <td><Button onclick={_ => onEditReview(c)} icon={<EditIcon />} /></td>
        </tr>)}
    </tbody>
  </ReviewedTable>
}

////#endregion

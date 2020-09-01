import styled from 'styled-components'
import { Dim, ColEx } from '../../common/Dim'
import { ChannelData, YtModel } from '../../common/YtModel'
import { ytTheme } from '../MainLayout'
import React, { useMemo } from 'react'
import { tagColor } from '../channel_relations/ChannelTags'
import ReactMarkdown from 'react-markdown'
import { Tag } from '../Tag'
import _ from 'lodash'
import { OptionProps } from 'react-select/lib/components/Option'
import { Review, BasicChannel } from '../../common/YtApi'

const chanDim = new Dim<ChannelData>(YtModel.channelDimStatic.meta)
const tagCol = chanDim.col('tags')
const tagFunc = { label: ColEx.labelFunc(tagCol), color: ColEx.colorFunc(tagCol) }


export interface ChannelReview {
  channel: BasicChannel
  review: Review
}

export interface Option {
  value?: string
  label?: string
}

export interface TagOption extends Option {
  md?: string
}

export const tagOptions: TagOption[] = _([
  { value: 'Conspiracy', md: 'Regularly promotes a variety of conspiracy theories. A conspiracy theory explains an evert/circumstance as the result of a secret plot that is not widely accepted to be true (even though sometimes it is). Example conspiracy theories: [Moon landings were faked](https://en.wikipedia.org/wiki/Moon_landing_conspiracy_theories), [QAnon](https://en.wikipedia.org/wiki/QAnon) & [Pizzagate](https://en.wikipedia.org/wiki/Pizzagate_conspiracy_theory), [Epstein was murdered](https://en.wikipedia.org/wiki/Death_of_Jeffrey_Epstein), [Trump-russia collusion](https://rationalwiki.org/wiki/Trump-Russia_connection). Examples: [X22Report](https://www.youtube.com/user/X22Report), [The Next News Network](https://www.youtube.com/user/NextNewsNetwork)' },
  { value: 'QAnon', md: 'A channel focused on [Q-Anon](https://en.wikipedia.org/wiki/QAnon). Q is a handle of someone with access to the "deep state" leaking plots against Trump and his supporters. Examples: [Edge of Wonder](https://www.youtube.com/channel/UCxC2RlwWGHnwXanvHNBmw2w), [prayingmedic](https://www.youtube.com/channel/UCSio3E7kYvPeHKhfuYZWriA)' },
  { value: 'Libertarian', md: 'A [political philosophy](https://en.wikipedia.org/wiki/Libertarianism) wth individual liberty as its main principal. Generally skeptical of authority and state power (e.g. regulation, taxes, government programs). Favor free markets and private ownership. To tag, this should be the main driver of their politics. Does not include libertarian socialists who also are anti-state but are anti-capitalist and promote communal living. Examples: [Reason](https://www.youtube.com/user/ReasonTV), [John Stossel](https://www.youtube.com/user/ReasonTV), [The Cato Institute](https://www.youtube.com/user/catoinstitutevideo)' },
  { value: 'AntiSJW', md: 'Significant focus on criticizing *Social Justice* (see below) with a positive view of the marketplace of ideas and discussing controversial topics. To tag, this should be a common focus in their content.|[MILO](https://www.youtube.com/user/yiannopoulosm), [Tim Pool](https://www.youtube.com/user/Timcasts)' },
  { value: 'SocialJustice', md: ' Believe or promote: Identity Politics & Intersectionality (narratives of oppression though the combination of historically oppressed identities), *Political Correctness* (the restriction of ideas and words you can say in polite society), *Social Constructionism* (the idea that the differences between individuals and groups are explained entirely by environment. For example sex differences are caused by culture not by biological sex).<br><br>Content in reaction to Anti-SJW or conservative content.<br><br>Their supporters are active on [r/Breadtube](https://www.reddit.com/r/BreadTube/) and the creators often identify with this label. This tag only includes breadtuber’s if their content is criticizing ant-SJW’s (promoting socialism is its own, separate tag). Examples: [Peter Coffin](https://www.youtube.com/user/petercoffin), [hbomberguy](https://www.youtube.com/user/hbomberguy)' },
  { value: 'WhiteIdentitarian', md: 'Identifies-with/is-proud-of the superiority of “whites” and western Civilization.<br><br>An example of identifying with “western heritage”  would be to refer to the sistine chapel, or bach as “our culture”.<br><br>Promotes or defends: An ethno-state where residence or citizenship would be limited to “whites” OR a type of nationalist that seek to maintain a white national identity (white nationalism), historical narratives focused on the “white” lineage and its superiority, Essentialist concepts of racial differences<br><br>Are concerned about whites becoming a minority population in the US. Examples: [NPI / RADIX](https://www.youtube.com/user/NPIAmerica), [Stefan Molyneux](https://www.youtube.com/user/stefbot)' },
  { value: 'Educational', md: 'Channel that has significant focuses on education material related to politics/culture. Examples: [TED](https://www.youtube.com/user/TEDtalksDirector/videos), [SoulPancake](https://www.youtube.com/user/soulpancake)' },
  { value: 'LateNightTalkShow', md: 'Channel with content presented humorous monologues about the day\'s news, guest interviews and comedy sketches. Examples: [Last Week Tonight](https://www.youtube.com/user/LastWeekTonight), [Trevor Noah](https://www.youtube.com/channel/UCwWhs_6x42TyRM4Wstoq8HA)' },
  { value: 'PartisanLeft', md: 'Mainly focused on politics and exclusively critical of Republicans. Would agree with this statement: “GOP policies are a threat to the well-being of the country“. Examples: [The Young Turks](https://www.youtube.com/user/TheYoungTurks), [CNN](https://www.youtube.com/user/CNN)' },
  { value: 'PartisanRight', md: ' Mainly focused on politics and exclusively critical of Democrats. Would agree with this statement: “Democratic policies threaten the nation”. Examples: [Fox News](https://www.youtube.com/user/FoxNewsChannel),[Candace Owens](https://www.youtube.com/channel/UCL0u5uz7KZ9q-pe-VC8TY-w)' },
  { value: 'AntiTheist', md: 'Self-identified atheist who are also actively critical of religion. Also called New Atheists or Street Epistemologists. Usually combined with an interest in philosophy. Examples:[Sam Harris](https://www.youtube.com/user/samharrisorg), [CosmicSkeptic](https://www.youtube.com/user/alexjoconnor), [Matt Dillahunty](https://www.youtube.com/user/SansDeity)' },
  { value: 'ReligiousConservative', md: 'A channel with a focus on promoting Christianity or Judaism in the context of politics and culture. Examples: [Ben Shapiro](https://www.youtube.com/channel/UCnQC_G5Xsjhp9fEJKuIcrSw), [PragerU](https://www.youtube.com/user/PragerUniversity)' },
  { value: 'Socialist', md: 'Focus on the problems of capitalism. Endorse the view that capitalism is the source of most problems in society. Critiques of aspects of capitalism that are more specific (i.e. promotion of fee healthcare or a large welfare system or public housing) don’t qualify for this tag. Promotes alternatives to capitalism. Usually some form of either  Social Anarchist  (stateless egalitarian communities) or Marxist (nationalized production and a way of viewing society though class relations and social conflict). Examples: [BadMouseProductions](https://www.youtube.com/user/xaxie1), [NonCompete](https://www.youtube.com/channel/UCkZFKKK-0YB0FvwoS8P7nHg/videos)' },
  { value: 'MRA', md: '(Men’s Rights Activist): Focus on advocating for rights for men. See men as the oppressed sex and will focus on examples where men are currently. Examples: [Karen Straughan](https://www.youtube.com/user/girlwriteswhat)' },
  { value: 'MissingLinkMedia', md: 'Channels funded by companies or venture capital, but not large enough to be considered “mainstream”. They are generally accepted as more credible than independent YouTube content. Examples: [Vox](https://www.youtube.com/user/voxdotcom) [NowThis News](https://www.youtube.com/user/nowthismedia)' },
  { value: 'StateFunded', md: 'Channels largely funded by a government. Examples:[PBS NewsHour](https://www.youtube.com/user/PBSNewsHour), [Al Jazeera](https://www.youtube.com/user/AlJazeeraEnglish), [RT](https://www.youtube.com/user/RussiaToday)' },
  { value: 'Mainstream News', md: 'Media institutions from TV, Cable or Newspaper that are also creating content for YouTube. Examples: [CNN](https://www.youtube.com/user/CNN), [Fox](https://www.youtube.com/user/FoxNewsChannel), [NYT](https://www.youtube.com/user/TheNewYorkTimes)' },
  { value: 'Politician', md: 'The channel is on behalf of a currently running/in-office Politician. Examples [Alexandria Ocasio-Cortez](https://www.youtube.com/channel/UCElqfal0wzzpLsHlRuqZjaA), [Donald J Trump](https://www.youtube.com/channel/UCAql2DyGU2un1Ei2nMYsqOA)' },
  { value: 'Black', md: 'Black creators focused on cultural/political issues of their community/identity (e.g. Police Violence, Racism). Examples:[African Diaspora News Channel](https://www.youtube.com/channel/UCKZGcrxRAhdUi58Mdr565mw), [Roland S. Martin](https://www.youtube.com/channel/UCjXB7nX8bL2U2sje8d212Yw), [Lisa Cabrera](https://www.youtube.com/channel/UCcTzK_2JDmFYGnUiaUleQYg) ' },
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

export const tagCustomOption = ({ innerRef, innerProps, isDisabled, data, isFocused, isSelected }: OptionProps<TagOption>) =>
  <TagDiv ref={innerRef} {...innerProps} className={[isSelected ? 'selected' : null, isFocused ? 'focused' : null].filter(n => n).join(' ')}>
    <div className='head'><label>{data.label}</label></div>
    <div className='md' style={{ padding: '0 1em' }}>
      <ReactMarkdown source={data.md} escapeHtml={false} renderers={{ link: CustomARender }} />
    </div>
  </TagDiv>


export function createChannelOptions(channels: _.Dictionary<BasicChannel>): {
  channelOptions: {
    label: string
    value: string
  }[]; channelDic: _.Dictionary<{
    label: string
    value: string
  }>
} {
  const channelOptions = _(channels)
    .map(c => ({ label: c.ChannelTitle, value: c.ChannelId }))
    .orderBy(c => c.label).value()
  const channelDic = _(channelOptions).keyBy(c => c.value).value()
  return { channelOptions, channelDic }
}

const lrCol = chanDim.col('lr')
const lrFunc = { label: ColEx.labelFunc(lrCol), color: ColEx.colorFunc(lrCol) }
export const lrOptions = lrCol.values.filter(v => v.value)

export const LrTag = ({ tag }: { tag: string }) => <Tag key={tag} label={lrFunc.label(tag)} color={tagColor(lrFunc.color(tag))} />
export const LrCustomLabel = ({ value }: any) => <LrTag tag={value} />

import * as React from "react"
import { useContext, useEffect, useState, useMemo, useCallback } from 'react'
import { UserContext, LoginOverlay } from '../UserContext'
import { channelsReviewed, saveReview as apiSaveReview, RawChannel, getChannels, Review } from '../../common/YtApi'
import { Spinner } from '../Spinner'
import { ytTheme, mainLayoutId } from '../MainLayout'
import styled from 'styled-components'
import _ from 'lodash'
import { jsonClone } from '../../common/Utils'
import { useToasts } from 'react-toast-notifications'
import Modal from 'react-modal'
import { ReviewForm } from './ReviewForm'
import { ReviewedGrid } from './ReviewGrid'
import { useHotkeys, Options as HotkeyOptions } from 'react-hotkeys-hook'
import { ChannelReview } from './ReviewCommon'

const ReviewPageDiv = styled.div`
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
const reviewValid = (r: Review): boolean => r.Relevance != null && r.LR != null

export const ReviewControl = () => {
  const { user } = useContext(UserContext)
  const [review, setReview] = useState<ChannelReview>(null)
  const [reviews, setReviews] = useState<ChannelReview[]>()
  const [channels, setChannels] = useState<_.Dictionary<RawChannel>>()
  const [pending, setPending] = useState<RawChannel[]>()
  const [editing, setEditing] = useState<ChannelReview>(null)

  const { addToast } = useToasts()

  const reviewedGrid = useMemo(
    () => <ReviewedGrid reviews={reviews} onEditReview={c => setEditing(jsonClone(c))} channels={channels} />,
    [reviews, channels])

  useEffect(() => {
    const go = async () => {
      const email = user?.email
      if (!email) return
      try {
        await init(email)
      } catch (e) {
        addToast(`unable to load reviews: ${e}`, { appearance: 'warning', autoDismiss: false })
      }
    }
    go()
  }, [user])

  const isEditing = editing != null
  const currentReview = isEditing ? editing : review
  const keyOption: HotkeyOptions = { filter: (e: KeyboardEvent) => true } //, scope: name

  const handlers = {
    'ctrl+s': () => { if (reviewValid(currentReview?.review)) saveReview(currentReview, isEditing) },
    'ctrl+d': () => { saveNonPoliticalReview(currentReview, isEditing) },
    'esc': () => setEditing(null)
  }


  _.forEach(handlers, (handler, key: keyof typeof handlers) => {
    useHotkeys(key, e => {
      e.preventDefault()
      e.stopImmediatePropagation()
      handler()
    }, keyOption, [editing, review])
  })

  const init = async (email: string) => {
    const channelsTask = getChannels()
    const reviewedTask = channelsReviewed(email)
    const channels = _.keyBy(await channelsTask, c => c.ChannelId)
    const reviews = _(await reviewedTask).map(r => ({ channel: channels[r.ChannelId], review: r })).value()
    const reviewedDic = _.keyBy(reviews, r => r.channel.ChannelId)
    const pending = _(channels).filter(c => c.ReviewStatus == 'Pending' && !reviewedDic[c.ChannelId]).value()
    const review = nextPending(pending)
    setChannels(channels)
    setReview(review)
    setReviews(reviews)
  }

  const nextPending = (pending: RawChannel[]): ChannelReview => {
    const c = pending.length > 0 ? pending[0] : null
    if (c)
      setPending(pending.filter(p => p.ChannelId != c.ChannelId))
    return {
      channel: c,
      review: c ? { ChannelId: c.ChannelId, SoftTags: [] } : null
    }
  }

  const saveReview = async ({ review, channel }: ChannelReview, isEditing: boolean): Promise<ChannelReview> => {
    const toSave = { review: { ...review, Updated: new Date().toISOString(), Email: user?.email }, channel }
    const res = await apiSaveReview(toSave.review)
    res.ok ? addToast(`Saved channel :  ${channel.ChannelTitle}`, { appearance: 'success', autoDismiss: true })
      : addToast(`Couldn't save:  ${await res.text()}`, { appearance: 'warning', autoDismiss: true })
    if (!res.ok) return
    setReviews(reviews.concat(toSave))
    if (!isEditing) setReview(nextPending(pending))
    else setEditing(null)
    return toSave
  }

  const saveNonPoliticalReview = ({ review, channel }: ChannelReview, isEditing: boolean) =>
    saveReview({ review: { ...review, Relevance: 0 }, channel }, isEditing)


  // fetch some channels for review & list existing
  return <ReviewPageDiv id='review-page'>
    <LoginOverlay verb='to review channels' />

    {user && <>

      {pending && <div>
        <h3>To Review ({pending.length})</h3>
        {pending.length == 0 && <span>You're up to date. You hard worker you!</span>}
      </div>}

      {(!channels || !reviews) ?
        <Spinner size='50px' /> :
        <ReviewForm
          review={review}
          channels={channels}
          onChange={r => setReview(r)}
          onSave={async r => { await saveReview(r, false) }}
          onSaveNonPolitical={async r => { await saveNonPoliticalReview(r, false) }}
          reviewValid={reviewValid}
        />}


      {reviews && <>
        <h3>Reviewed</h3>
        <div>{reviews?.length <= 0 ? <>You haven't reviewed anything yet</> : reviewedGrid}</div>
      </>
      }

      {isEditing && <Modal
        isOpen={isEditing}
        ariaHideApp={false}
        parentSelector={() => document.querySelector('#review-page')}
        style={{
          overlay: {
            backgroundColor: 'none',
            backdropFilter: 'blur(15px)'
          },
          content: {
            backgroundColor: ytTheme.backColor, padding: '2em', border: 'none',
            maxWidth: '800px',
            minWidth: "600px",
            top: '50%',
            left: '50%',
            right: 'auto',
            bottom: 'auto',
            marginRight: '-50%',
            transform: 'translate(-50%, -50%)'
          }
        }}>
        <ReviewForm
          review={editing}
          channels={channels}
          onChange={r => setEditing(r)}
          onSave={async r => {
            await saveReview(r, true)
            setEditing(null)
          }}
          onSaveNonPolitical={async r => { await saveNonPoliticalReview(r, true) }}
          reviewValid={reviewValid}
          onCancel={() => setEditing(null)}
        />
      </Modal>}
    </>
    }
  </ReviewPageDiv >
}
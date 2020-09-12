import * as React from "react"
import { useContext, useEffect, useState, useMemo, useCallback } from 'react'
import { UserContext, LoginOverlay } from '../UserContext'
import { channelsReviewed, saveReview as apiSaveReview, BasicChannel, reviewChannelLists, Review, ChannelReview, ChannelTitle, channelSearch, getChannels } from '../../common/YtApi'
import { Spinner } from '../Spinner'
import { ytTheme, mainLayoutId, selectStyle, selectTheme } from '../MainLayout'
import styled from 'styled-components'
import _ from 'lodash'
import { jsonClone } from '../../common/Utils'
import { useToasts } from 'react-toast-notifications'
import Modal from 'react-modal'
import { ReviewForm } from './ReviewForm'
import { ReviewedGrid } from './ReviewGrid'
import { useHotkeys, Options as HotkeyOptions } from 'react-hotkeys-hook'
import { createChannelOptions, Option, fieldSizes, Field, FormStyle, loadChannelOptions, ChannelOption } from './ReviewCommon'
import Select from 'react-select'
import Async from 'react-select/async'
import { EsContext } from '../SearchContext'

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
  hr { border-color: ${ytTheme.backColorBolder3}  }
`

const reviewValid = (r: Review): boolean => r.relevance != null && r.lr != null

export const ReviewControl = () => {
  const { user } = useContext(UserContext)
  const [review, setReview] = useState<ChannelReview>(null)
  const [reviews, setReviews] = useState<Review[]>()
  const [reviewLists, setReviewsLists] = useState<_.Dictionary<ChannelTitle[]>>(null)
  const [reviewListName, setReviewListName] = useState<string>('auto')
  const [pending, setPending] = useState<ChannelTitle[]>()
  const [editing, setEditing] = useState<ChannelReview>(null)
  const [reviewsPage, setReviewPage] = useState<number>(1)
  const [reviewsShown, setReviewsShown] = useState<ChannelReview[]>()

  const { addToast } = useToasts()

  const newReview = (c: ChannelTitle): ChannelReview => c ? ({
    channel: c,
    review: c ? { channelId: c.channelId, softTags: [] } : null
  }) : null

  useEffect(() => {
    const go = async () => {
      const email = user?.email
      if (!email) return
      try {
        await init(email)
      } catch (e) {
        addToast(`unable to load reviews: ${e}`, { appearance: 'warning', autoDismiss: false })
        console.log('unable to load reviews', e)
      }
    }
    go()
  }, [user])

  const esCfg = useContext(EsContext)
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

  const channelIsPending = (c: ChannelTitle, reviews: Review[]) => reviews && !reviews.find(r => r.channelId == c.channelId)

  const init = async (email: string) => {
    const reviewListsTask = reviewChannelLists()
    const reviewedTask = channelsReviewed(email)
    const reviews = _(await reviewedTask)
      .orderBy(r => r.updated, 'desc')
      .value()

    const reviewLists = await reviewListsTask
    setReviewsLists(reviewLists)
    updateReviewAndPending(reviewListName, reviewLists, reviews)
    setReviews(reviews)
  }

  const updateReviewAndPending = (listName?: string, lists?: _.Dictionary<ChannelTitle[]>, reviewsParam?: Review[]) => {
    listName = listName ?? reviewListName
    lists = lists ?? reviewLists
    reviewsParam = reviewsParam ?? reviews
    const newPending = _(lists[listName]).filter(c => channelIsPending(c, reviewsParam)).value()
    // take one at random to try and avoid too many double up reviews from different people
    const c = _(newPending).shuffle().head()
    setReview(newReview(c))
    setPending(newPending)
  }

  const saveReview = async ({ review, channel }: ChannelReview, isEditing: boolean): Promise<Review> => {
    const toSave = { ...review, updated: new Date().toISOString(), email: user?.email }
    const res = await apiSaveReview(toSave)
    res.ok ? addToast(`Saved channel :  ${channel.channelTitle}`, { appearance: 'success', autoDismiss: true })
      : addToast(`Couldn't save:  ${await res.text()}`, { appearance: 'warning', autoDismiss: true })
    if (!res.ok) return
    const newReviews = reviews.filter(r => r.channelId != toSave.channelId).concat(toSave)
    setReviews(newReviews)
    if (!isEditing) updateReviewAndPending(reviewListName, reviewLists, newReviews)
    else setEditing(null)
    return toSave
  }

  const saveNonPoliticalReview = ({ review, channel }: ChannelReview, isEditing: boolean) =>
    saveReview({ review: { ...review, relevance: 0 }, channel }, isEditing)

  useEffect(() => {
    if (!user || !reviews) return
    const go = async () => {
      const rShown = _(reviews)
        .groupBy(r => r.channelId).map(g => _(g).orderBy(r => r.updated, 'desc').head()) // remove dupes
        .orderBy(r => r.updated, 'desc').slice(0, reviewsPage * 50).value() // take recent 50
      const ids = _(rShown).flatMap(c => [c.channelId, c.mainChannelId]).filter(id => id != null).uniq().value()
      const channels = _(await getChannels(esCfg, ids)).keyBy(r => r.channelId).value()
      const crShown: ChannelReview[] = rShown.map(r => (
        {
          review: r,
          channel: channels[r.channelId],
          mainChannel: r.mainChannelId ? channels[r.mainChannelId] : null
        }))
      setReviewsShown(crShown)
    }
    go()
  }, [reviews, reviewsPage, user])

  const reviewedGrid = useMemo(
    () => <><ReviewedGrid
      reviews={reviewsShown}
      onEditReview={c => setEditing(jsonClone(c))} />
      {reviewsShown && reviews && reviewsShown.length < reviews.length && <div>
        <a onClick={() => setReviewPage(reviewsPage + 1)}>show more reviews</a>
      </div>}
    </>,
    [reviewsShown])

  const listNameOptions: Option[] = _.keys(reviewLists).map(k => ({ value: k, label: k }))
  const pendingOptions: ChannelOption[] = pending?.map(p => ({ value: p.channelId, label: p.channelTitle, channel: p }))

  // fetch some channels for review & list existing
  return <ReviewPageDiv id='review-page'>
    <LoginOverlay verb='to review channels' />

    {user && <>

      {pending && <FormStyle space='.8em'>
        <Field label={`Review queue (${pendingOptions?.length})`} size='l' >
          <Select
            value={listNameOptions.find(o => o.value == reviewListName)}
            onChange={(o: Option) => {
              setReviewListName(o.value)
              updateReviewAndPending(o.value)
            }}
            options={listNameOptions}
            styles={selectStyle} theme={selectTheme}
          />
        </Field>



        <Field label={`Reviewing`} size='l'>
          <Async
            value={review?.channel ? { value: review.review?.channelId, label: review.channel?.channelTitle, channel: review.channel } : null}
            isClearable
            backspaceRemovesValue
            loadOptions={s => loadChannelOptions(esCfg, s)}
            defaultOptions={pendingOptions}
            onChange={(o: ChannelOption) => setReview(newReview(o?.channel))}
            styles={selectStyle} theme={selectTheme}
          />
        </Field>

        {pending.length == 0 && <span>You're up to date. You hard worker you!</span>}
      </FormStyle>}

      <hr style={{ margin: '0.5em 0 ' }} />

      {!reviews ?
        <Spinner size='50px' /> :
        <ReviewForm
          review={review}
          onChange={r => setReview(r)}
          onSave={async r => { await saveReview(r, false) }}
          onSkip={() => updateReviewAndPending()}
          onSaveNonPolitical={async r => { await saveNonPoliticalReview(r, false) }}
          reviewValid={reviewValid}
        />}

      <hr style={{ margin: '0.5em 0 ' }} />

      {reviews && <>
        <h3>Create/override review</h3>
        {/* {reviewChannelSelect} */}
      </>}

      {reviews && <>
        <h3>Reviewed</h3>
        <div>
          {reviews?.length <= 0 ?
            <>You haven't reviewed anything yet</> : reviewedGrid
          }
        </div>
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

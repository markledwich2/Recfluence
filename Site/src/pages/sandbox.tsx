import * as React from "react"
import { MainLayout } from "../components/MainLayout"
import { Button } from "../components/Button"
import { FilterList, Lock } from '@styled-icons/material'

const App = () => (
    <MainLayout>

        <div>

            <Button label="filter" icon={<FilterList />} onclick={(e) => console.log('clicked', e)} />
            <Button label="lock" icon={<Lock />} onclick={(e) => console.log('clicked', e)} />
            <Button label="texthing" icon={<Lock />} onclick={(e) => console.log('clicked', e)} />

        </div>

    </MainLayout>
)

export default App
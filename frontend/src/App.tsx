import { useState } from 'react'
import reactLogo from './assets/react.svg'
import viteLogo from '/vite.svg'
import './App.css'
import { createBrowserRouter, RouterProvider } from 'react-router-dom'
import Home from './page/Home/Home'

const router = createBrowserRouter([

  {
    path: "/",
    element: <Home/>
  }
])

function App() {

  return (
    <>
          <div>

          <RouterProvider router={router} />

          </div>
    </>
  )
}

export default App

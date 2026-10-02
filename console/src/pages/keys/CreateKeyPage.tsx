import { useEffect } from 'react'
import { useNavigate } from 'react-router-dom'

export function CreateKeyPage() {
  const navigate = useNavigate()

  useEffect(() => {
    // Redirect to keys page with create modal open
    navigate('/keys?create=true', { replace: true })
  }, [navigate])

  return null
}
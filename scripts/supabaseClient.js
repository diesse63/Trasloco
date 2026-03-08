import { createClient } from 'https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2/+esm'

const supabaseUrl = 'https://rywhhccxhlcmfbjtfkjs.supabase.co'
const supabaseKey = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJ5d2hoY2N4aGxjbWZianRma2pzIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzI3MjAxOTYsImV4cCI6MjA4ODI5NjE5Nn0.hVdTTvrtoJZfRgmd8eAzTyA_cu43E-84l9ZIOvjOnN0'

export const supabase = createClient(supabaseUrl, supabaseKey)
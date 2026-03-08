async function testDB() {
    const { supabase } = await import('./supabaseClient.js')
    console.log("==== TEST DATABASE TRASLOCO ====")

    // Test Stanze
    const { data: stanze, error: errStanze } = await supabase.from('stanze').select('*')
    if(errStanze) console.error("Errore Stanze:", errStanze)
    else console.log("Stanze:", stanze)

    // Test Mobili
    const { data: mobili, error: errMobili } = await supabase
        .from('mobili')
        .select('*, stanza:stanze!mobili_idstanza_fkey(nome)')
    if(errMobili) console.error("Errore Mobili:", errMobili)
    else console.log("Mobili:", mobili)

    // Test Scatole
    const { data: scatole, error: errScatole } = await supabase
        .from('scatole')
        .select('*')
    if(errScatole) console.error("Errore Scatole:", errScatole)
    else console.log("Scatole:", scatole)

    // Test Stanze presenti per scatola (view)
    const { data: scatoleStanze, error: errScatoleStanze } = await supabase
        .from('vista_scatole_stanze')
        .select('*')
    if(errScatoleStanze) console.error("Errore Vista Scatole/Stanze:", errScatoleStanze)
    else console.log("Vista Scatole/Stanze:", scatoleStanze)

    // Test Oggetti
    const { data: oggetti, error: errOggetti } = await supabase
        .from('oggetti')
        .select('*, scatola:scatole!oggetti_idscatola_fkey(*), stanza:stanze!oggetti_idstanza_fkey(nome), mobile:mobili!oggetti_idmobile_fkey(nome)')
    if(errOggetti) console.error("Errore Oggetti:", errOggetti)
    else console.log("Oggetti:", oggetti)

    console.log("==== FINE TEST ====")
}

testDB().catch((error) => {
    console.error('Errore configurazione/test:', error.message)
    console.error('Suggerimento 1: crea/compila il file .env nella root del progetto con:')
    console.error('SUPABASE_URL=https://<project-ref>.supabase.co')
    console.error('SUPABASE_ANON_KEY=<la-tua-anon-key>')
    console.error('Suggerimento 2: in alternativa, in PowerShell imposta le variabili prima del test:')
    console.error('$env:SUPABASE_URL="https://<project-ref>.supabase.co"')
    console.error('$env:SUPABASE_ANON_KEY="<la-tua-anon-key>"')
})
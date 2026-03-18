import 'dotenv/config';
import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import { createClient } from '@supabase/supabase-js';

const app = express();
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.SUPABASE_ANON_KEY;
const supabase = supabaseUrl && supabaseKey ? createClient(supabaseUrl, supabaseKey) : null;

app.use(express.json({ limit: '15mb' }));
app.use(express.static(__dirname));

function ensureDbConfigured(res) {
    if (!supabase) {
        res.status(500).json({
            error: 'Database non configurato. Imposta SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY (o SUPABASE_ANON_KEY) nel file .env.'
        });
        return false;
    }
    return true;
}

function parseId(rawId) {
    const parsed = Number(rawId);
    return Number.isInteger(parsed) && parsed > 0 ? parsed : null;
}

function handleDbError(res, error) {
    res.status(400).json({ error: error.message || 'Errore database.' });
}

async function hasRelatedRows(table, column, id) {
    const { count, error } = await supabase
        .from(table)
        .select('*', { head: true, count: 'exact' })
        .eq(column, id);
    if (error) throw error;
    return (count || 0) > 0;
}

app.get('/api/stanze', async (_req, res) => {
    if (!ensureDbConfigured(res)) return;
    const { data, error } = await supabase.from('stanze').select('*').order('id', { ascending: true });
    if (error) return handleDbError(res, error);
    res.json(data || []);
});

app.post('/api/stanze', async (req, res) => {
    if (!ensureDbConfigured(res)) return;
    const nome = (req.body?.nome || '').trim();
    const note = (req.body?.note || '').trim() || null;
    if (!nome) return res.status(400).json({ error: 'Il nome stanza e obbligatorio.' });

    const { data, error } = await supabase.from('stanze').insert({ nome, note }).select('*').single();
    if (error) return handleDbError(res, error);
    res.status(201).json(data);
});

app.put('/api/stanze/:id', async (req, res) => {
    if (!ensureDbConfigured(res)) return;
    const id = parseId(req.params.id);
    if (!id) return res.status(400).json({ error: 'ID stanza non valido.' });

    const nome = (req.body?.nome || '').trim();
    const note = (req.body?.note || '').trim() || null;
    if (!nome) return res.status(400).json({ error: 'Il nome stanza e obbligatorio.' });

    const { data, error } = await supabase.from('stanze').update({ nome, note }).eq('id', id).select('*').single();
    if (error) return handleDbError(res, error);
    res.json(data);
});

app.delete('/api/stanze/:id', async (req, res) => {
    if (!ensureDbConfigured(res)) return;
    const id = parseId(req.params.id);
    if (!id) return res.status(400).json({ error: 'ID stanza non valido.' });

    try {
        const hasMobili = await hasRelatedRows('mobili', 'idstanza', id);
        const hasOggetti = await hasRelatedRows('oggetti', 'idstanza', id);
        if (hasMobili || hasOggetti) {
            return res.status(409).json({ error: 'Impossibile eliminare la stanza: esistono mobili o oggetti collegati.' });
        }
    } catch (error) {
        return handleDbError(res, error);
    }

    const { error } = await supabase.from('stanze').delete().eq('id', id);
    if (error) return handleDbError(res, error);
    res.status(204).send();
});

app.get('/api/mobili', async (_req, res) => {
    if (!ensureDbConfigured(res)) return;
    const { data, error } = await supabase
        .from('mobili')
        .select('*, stanza:stanze!mobili_idstanza_fkey(nome)')
        .order('id', { ascending: true });
    if (error) return handleDbError(res, error);
    res.json(data || []);
});

app.post('/api/mobili', async (req, res) => {
    if (!ensureDbConfigured(res)) return;
    const nome = (req.body?.nome || '').trim();
    const idstanza = parseId(req.body?.idstanza);
    const note = (req.body?.note || '').trim() || null;
    if (!nome || !idstanza) return res.status(400).json({ error: 'Nome mobile e stanza sono obbligatori.' });

    const { data, error } = await supabase
        .from('mobili')
        .insert({ nome, idstanza, note })
        .select('*, stanza:stanze!mobili_idstanza_fkey(nome)')
        .single();
    if (error) return handleDbError(res, error);
    res.status(201).json(data);
});

app.put('/api/mobili/:id', async (req, res) => {
    if (!ensureDbConfigured(res)) return;
    const id = parseId(req.params.id);
    if (!id) return res.status(400).json({ error: 'ID mobile non valido.' });

    const nome = (req.body?.nome || '').trim();
    const idstanza = parseId(req.body?.idstanza);
    const note = (req.body?.note || '').trim() || null;
    if (!nome || !idstanza) return res.status(400).json({ error: 'Nome mobile e stanza sono obbligatori.' });

    const { data, error } = await supabase
        .from('mobili')
        .update({ nome, idstanza, note })
        .eq('id', id)
        .select('*, stanza:stanze!mobili_idstanza_fkey(nome)')
        .single();
    if (error) return handleDbError(res, error);
    res.json(data);
});

app.delete('/api/mobili/:id', async (req, res) => {
    if (!ensureDbConfigured(res)) return;
    const id = parseId(req.params.id);
    if (!id) return res.status(400).json({ error: 'ID mobile non valido.' });

    try {
        const hasOggetti = await hasRelatedRows('oggetti', 'idmobile', id);
        if (hasOggetti) {
            return res.status(409).json({ error: 'Impossibile eliminare il mobile: esistono oggetti collegati.' });
        }
    } catch (error) {
        return handleDbError(res, error);
    }

    const { error } = await supabase.from('mobili').delete().eq('id', id);
    if (error) return handleDbError(res, error);
    res.status(204).send();
});

app.get('/api/scatole', async (_req, res) => {
    if (!ensureDbConfigured(res)) return;
    const { data, error } = await supabase
        .from('scatole')
        .select('*')
        .order('id', { ascending: true });
    if (error) return handleDbError(res, error);
    res.json(data || []);
});

app.post('/api/scatole', async (req, res) => {
    if (!ensureDbConfigured(res)) return;
    const nome = (req.body?.nome || '').trim() || null;
    const pathfoto = (req.body?.pathfoto || '').trim() || null;
    const note = (req.body?.note || '').trim() || null;
    const payload = { nome, pathfoto, note };
    const { data, error } = await supabase
        .from('scatole')
        .insert(payload)
        .select('*')
        .single();

    if (error) return handleDbError(res, error);
    res.status(201).json(data);
});

app.put('/api/scatole/:id', async (req, res) => {
    if (!ensureDbConfigured(res)) return;
    const id = parseId(req.params.id);
    if (!id) return res.status(400).json({ error: 'ID scatola non valido.' });

    const nome = (req.body?.nome || '').trim() || null;
    const pathfoto = (req.body?.pathfoto || '').trim() || null;
    const note = (req.body?.note || '').trim() || null;
    const payload = { nome, pathfoto, note };
    const { data, error } = await supabase
        .from('scatole')
        .update(payload)
        .eq('id', id)
        .select('*')
        .single();

    if (error) return handleDbError(res, error);
    res.json(data);
});

app.delete('/api/scatole/:id', async (req, res) => {
    if (!ensureDbConfigured(res)) return;
    const id = parseId(req.params.id);
    if (!id) return res.status(400).json({ error: 'ID scatola non valido.' });

    try {
        const hasOggetti = await hasRelatedRows('oggetti', 'idscatola', id);
        if (hasOggetti) {
            return res.status(409).json({ error: 'Impossibile eliminare la scatola: esistono oggetti collegati.' });
        }
    } catch (error) {
        return handleDbError(res, error);
    }

    const { error } = await supabase.from('scatole').delete().eq('id', id);
    if (error) return handleDbError(res, error);
    res.status(204).send();
});

app.get('/', (_req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

app.listen(4000, () => {
    console.log('App Trasloco in esecuzione su http://localhost:4000');
    if (!supabase) {
        console.warn('Supabase non configurato: crea .env con SUPABASE_URL e SUPABASE_SERVICE_ROLE_KEY (o SUPABASE_ANON_KEY).');
    }
});
import { supabase } from './supabaseClient.js';

const STORAGE_BUCKET_KEY = 'trasloco.storageBucket';
const PREFERRED_STORAGE_BUCKET = 'Oggetti';
const STORAGE_BUCKET_FALLBACKS = [PREFERRED_STORAGE_BUCKET, 'oggetti', 'publics', 'public', 'trasloco', 'uploads', 'images', 'immagini'];
const GESTIONE_PREFILL_KEY = 'trasloco.gestionePrefill';
const DRIVE_FOLDER_ID = '1X__ukmJsar5sShvx67KCzKpj-wA03Zv7';
const DRIVE_BACKUP_PREFIX = 'trasloco-backup-';
const DRIVE_RETENTION_COUNT = 3;
const DRIVE_SCOPE = 'https://www.googleapis.com/auth/drive.file';
const DRIVE_CLIENT_ID_STORAGE_KEY = 'trasloco.driveClientId';

const driveState = {
    accessToken: '',
    tokenExpiresAt: 0,
    clientId: '',
};

const storageState = {
    bucket: (() => {
        try {
            const savedBucket = window.localStorage.getItem(STORAGE_BUCKET_KEY);
            if (!savedBucket) return PREFERRED_STORAGE_BUCKET;
            return savedBucket.toLowerCase() === PREFERRED_STORAGE_BUCKET.toLowerCase()
                ? savedBucket
                : PREFERRED_STORAGE_BUCKET;
        } catch {
            return PREFERRED_STORAGE_BUCKET;
        }
    })(),
};

function setCurrentStorageBucket(bucketName) {
    if (!bucketName) return;
    storageState.bucket = bucketName;
    try {
        window.localStorage.setItem(STORAGE_BUCKET_KEY, bucketName);
    } catch {
        // Ignore storage persistence errors.
    }
}

function isBucketNotFoundError(error) {
    return /bucket\s+not\s+found/i.test(error?.message || '');
}

function isStorageObjectNotFoundError(error) {
    return /not\s+found|no\s+such\s+object/i.test(error?.message || '');
}

function getStorageBucketCandidates(extra = []) {
    const unique = new Set([
        storageState.bucket,
        ...STORAGE_BUCKET_FALLBACKS,
        ...extra,
    ]);
    return Array.from(unique).filter(Boolean);
}

async function fetchBucketNames() {
    const { data, error } = await supabase.storage.listBuckets();
    if (error || !Array.isArray(data)) return [];
    return data.map((bucket) => bucket.name).filter(Boolean);
}

async function uploadWithBucketFallback(path, fileBody, options = {}) {
    const candidateSets = [getStorageBucketCandidates()];
    const attemptedBuckets = [];
    let lastError = null;

    for (const candidates of candidateSets) {
        for (const bucketName of candidates) {
            attemptedBuckets.push(bucketName);
            const result = await supabase.storage.from(bucketName).upload(path, fileBody, options);
            if (!result.error) {
                setCurrentStorageBucket(bucketName);
                return { ...result, bucket: bucketName, attemptedBuckets };
            }

            lastError = result.error;
            if (!isBucketNotFoundError(result.error)) {
                return { ...result, bucket: bucketName, attemptedBuckets };
            }
        }

        if (candidateSets.length === 1) {
            const bucketNamesFromApi = await fetchBucketNames();
            const additional = getStorageBucketCandidates(bucketNamesFromApi)
                .filter((name) => !candidates.includes(name));
            if (additional.length > 0) candidateSets.push(additional);
        }
    }

    return { data: null, error: lastError || new Error('Nessun bucket storage disponibile'), bucket: null, attemptedBuckets };
}

async function removeWithBucketFallback(path, extra = []) {
    if (!path) return { error: null, attemptedBuckets: [] };

    const candidateSets = [getStorageBucketCandidates(extra)];
    const attemptedBuckets = [];
    let lastError = null;

    for (const candidates of candidateSets) {
        for (const bucketName of candidates) {
            attemptedBuckets.push(bucketName);
            const result = await supabase.storage.from(bucketName).remove([path]);
            if (!result.error || isStorageObjectNotFoundError(result.error)) {
                setCurrentStorageBucket(bucketName);
                return { error: null, attemptedBuckets, bucket: bucketName };
            }

            lastError = result.error;
            if (!isBucketNotFoundError(result.error)) {
                return { error: result.error, attemptedBuckets, bucket: bucketName };
            }
        }

        if (candidateSets.length === 1) {
            const bucketNamesFromApi = await fetchBucketNames();
            const additional = getStorageBucketCandidates(bucketNamesFromApi)
                .filter((name) => !candidates.includes(name));
            if (additional.length > 0) candidateSets.push(additional);
        }
    }

    return { error: lastError || new Error('Nessun bucket storage disponibile'), attemptedBuckets, bucket: null };
}

// Funzione per caricare i pannelli
async function caricaPannello(nome) {
    const contenitore = document.getElementById('contenitorePannelli');
    if (!contenitore) return;

    try {
        const resp = await fetch(`pannelli/${nome}.html`);
        if (!resp.ok) throw new Error('File pannello non trovato');
        contenitore.innerHTML = await resp.text();

        if (nome === 'inserimento') initInserimento();
        if (nome === 'gestione') initGestione();
        if (nome === 'stanze') initServizi('stanze');
        if (nome === 'scatole') initServizi('scatole');
    } catch (err) {
        contenitore.innerHTML = `<p style="color:red; padding:20px;">Errore: ${err.message}</p>`;
    }
}

document.addEventListener('DOMContentLoaded', () => {
    const btnIns = document.getElementById('nav-inserimento');
    const btnGes = document.getElementById('nav-gestione');
    const btnStanze = document.getElementById('nav-stanze');
    const btnScatole = document.getElementById('nav-scatole');

    if (btnIns) btnIns.onclick = () => caricaPannello('inserimento');
    if (btnGes) btnGes.onclick = () => caricaPannello('gestione');
    if (btnStanze) btnStanze.onclick = () => caricaPannello('stanze');
    if (btnScatole) btnScatole.onclick = () => caricaPannello('scatole');

    initNetworkBadge();
    initBackupControl();
    caricaPannello('inserimento');
});

function initNetworkBadge() {
    const badge = document.getElementById('networkBadge');
    if (!badge) return;

    const updateNetworkBadge = () => {
        const online = navigator.onLine;
        badge.textContent = online ? 'Online' : 'Offline';
        badge.classList.toggle('is-online', online);
        badge.classList.toggle('is-offline', !online);
    };

    window.addEventListener('online', updateNetworkBadge);
    window.addEventListener('offline', updateNetworkBadge);
    updateNetworkBadge();
}

// --- LOGICA INSERIMENTO ---
async function initInserimento() {
    const stepNodes = Array.from(document.querySelectorAll('[data-step]'));
    const stepDots = Array.from(document.querySelectorAll('[data-step-dot]'));
    const btnPrevStep = document.getElementById('btnPrevStep');
    const btnNextStep = document.getElementById('btnNextStep');
    const selStanza = document.getElementById('selStanza');
    const selMobile = document.getElementById('selMobile');
    const selScatola = document.getElementById('selScatola');
    const btnCreateScatola = document.getElementById('btnCreateScatola');
    const scatolaModeMsg = document.getElementById('scatolaModeMsg');
    const inputFoto = document.getElementById('fotoOggetto');
    const preview = document.getElementById('preview');
    const form = document.getElementById('formOggetto');
    const imgApprovalMsg = document.getElementById('imgApprovalMsg');
    const summaryFoto = document.getElementById('summaryFoto');
    const summaryScatola = document.getElementById('summaryScatola');
    const summaryStanza = document.getElementById('summaryStanza');
    const summaryMobile = document.getElementById('summaryMobile');
    const btnSave = form?.querySelector('button[type="submit"]');
    const noteOggetto = document.getElementById('noteOggetto');
    const acqResultPanel = document.getElementById('acqResultPanel');
    const acqResultImage = document.getElementById('acqResultImage');
    const acqResultNome = document.getElementById('acqResultNome');
    const acqResultScatola = document.getElementById('acqResultScatola');
    const acqResultStanza = document.getElementById('acqResultStanza');
    const acqResultMobile = document.getElementById('acqResultMobile');
    const acqResultNote = document.getElementById('acqResultNote');
    const btnNewAcquisition = document.getElementById('btnNewAcquisition');
    const btnDeleteAcquisition = document.getElementById('btnDeleteAcquisition');
    const LAST_SCATOLA_BY_STANZA_KEY = 'trasloco.lastScatolaByStanza';

    let imageApproved = false;
    let currentStep = 1;
    let lastSavedAcquisition = null;
    let isCreatingScatola = false;
    let isSavingOggetto = false;
    let isDeletingAcquisition = false;

    const showResultPanel = (show) => {
        if (form) form.hidden = show;
        if (acqResultPanel) acqResultPanel.hidden = !show;
    };

    const setImageApproval = (approved, message) => {
        imageApproved = approved;
        if (!imgApprovalMsg) return;
        imgApprovalMsg.textContent = message;
        imgApprovalMsg.style.color = approved ? '#047857' : '#b91c1c';
        if (summaryFoto) summaryFoto.textContent = approved ? 'Acquisita' : 'Non caricata';
    };

    const refreshSummary = () => {
        if (summaryScatola && selScatola) {
            const scatolaText = selScatola.options[selScatola.selectedIndex]?.textContent?.trim();
            summaryScatola.textContent = scatolaText || 'Non selezionata';
        }
        if (summaryStanza && selStanza) {
            const stanzaText = selStanza.options[selStanza.selectedIndex]?.textContent?.trim();
            summaryStanza.textContent = stanzaText || 'Non selezionata';
        }
        if (summaryMobile && selMobile) {
            const mobileText = selMobile.options[selMobile.selectedIndex]?.textContent?.trim();
            summaryMobile.textContent = mobileText || 'Nessuno';
        }
    };

    const renderWizardStep = () => {
        stepNodes.forEach((node) => {
            const isActive = Number(node.dataset.step) === currentStep;
            node.classList.toggle('is-active', isActive);
        });

        stepDots.forEach((dot) => {
            const step = Number(dot.dataset.stepDot);
            dot.classList.toggle('is-active', step === currentStep);
            dot.classList.toggle('is-complete', step < currentStep);
        });

        if (btnPrevStep) btnPrevStep.disabled = currentStep === 1;
        if (btnNextStep) btnNextStep.style.display = currentStep === 4 ? 'none' : '';
        if (btnSave) btnSave.style.display = currentStep === 4 ? '' : 'none';
        refreshSummary();
    };

    const validateCurrentStep = () => {
        if (currentStep === 1 && !inputFoto?.files?.[0]) {
            window.alert('Scatta o carica una foto prima di continuare.');
            return false;
        }
        if (currentStep === 2 && !selStanza?.value) {
            window.alert('Seleziona la stanza prima di continuare.');
            return false;
        }
        if (currentStep === 3) {
            const scatolaId = Number(selScatola?.value || 0);
            if (!scatolaId) {
                window.alert('Seleziona una scatola prima di continuare.');
                return false;
            }
        }
        return true;
    };

    const goToStep = (nextStep) => {
        currentStep = Math.max(1, Math.min(4, nextStep));
        renderWizardStep();
    };

    const getLastScatolaMap = () => {
        try {
            return JSON.parse(window.localStorage.getItem(LAST_SCATOLA_BY_STANZA_KEY) || '{}');
        } catch {
            return {};
        }
    };

    const saveLastScatolaForStanza = (stanzaId, scatolaId) => {
        if (!stanzaId || !scatolaId) return;
        const map = getLastScatolaMap();
        map[String(stanzaId)] = Number(scatolaId);
        try {
            window.localStorage.setItem(LAST_SCATOLA_BY_STANZA_KEY, JSON.stringify(map));
        } catch {
            // Ignore localStorage errors.
        }
    };

    const loadMobiliByStanza = async (stanzaId) => {
        if (!selMobile) return;
        selMobile.innerHTML = '<option value="">Nessuno</option>';
        if (!stanzaId) return;

        const { data: mobili, error } = await supabase
            .from('mobili')
            .select('id,nome')
            .eq('idstanza', stanzaId)
            .order('nome', { ascending: true });

        if (error) {
            window.alert(`Caricamento mobili fallito: ${error.message}`);
            return;
        }

        selMobile.innerHTML = '<option value="">Nessuno</option>' +
            (mobili || []).map(m => `<option value="${m.id}">${escapeHtml(m.nome)}</option>`).join('');
    };

    const loadScatoleAll = async () => {
        if (!selScatola) return;
        selScatola.innerHTML = '<option value="">Seleziona scatola</option>';

        const { data: scatole, error } = await supabase
            .from('scatole')
            .select('id,nome')
            .order('id', { ascending: false });

        if (error) {
            window.alert(`Caricamento scatole fallito: ${error.message}`);
            return;
        }

        selScatola.innerHTML = '<option value="">Seleziona scatola</option>' +
            (scatole || []).map(s => `<option value="${s.id}">${escapeHtml(s.nome || String(s.id))}</option>`).join('');
        refreshSummary();
    };

    const createNewScatola = async (_stanzaId, mobileId) => {
        const insertRes = await supabase
            .from('scatole')
            .insert({
                nome: null,
                note: null,
                pathfoto: null,
            })
            .select('id')
            .single();

        if (insertRes.error || !insertRes.data?.id) {
            window.alert(`Creazione scatola fallita: ${insertRes.error?.message || 'ID non disponibile'}`);
            return null;
        }

        const newScatolaId = Number(insertRes.data.id);
        const updateNameRes = await supabase
            .from('scatole')
            .update({ nome: String(newScatolaId) })
            .eq('id', newScatolaId);

        if (updateNameRes.error) {
            window.alert(`Scatola creata ma nome non impostato: ${updateNameRes.error.message}`);
            return null;
        }

        // Nel nuovo modello la scatola non memorizza mobile/stanza.
        void mobileId;

        window.alert(`Conferma creazione scatola: nome assegnato ${newScatolaId}.`);
        return newScatolaId;
    };

    const resetWizardForNewAcquisition = async () => {
        setImageApproval(false, 'Foto non acquisita.');
        if (form) form.reset();
        goToStep(1);
        if (preview) {
            preview.src = '';
            preview.style.display = 'none';
        }

        await Promise.all([
            loadScatoleAll(),
            loadMobiliByStanza(''),
        ]);

        refreshSummary();
        if (scatolaModeMsg) {
            scatolaModeMsg.textContent = 'Seleziona una scatola esistente (puo contenere oggetti di stanze diverse). Per crearne una nuova, scegli prima la stanza al passo 2.';
            scatolaModeMsg.style.color = '#334155';
        }
        showResultPanel(false);
    };

    if (!selStanza) return;

    const { data: stanze, error: stanzeError } = await supabase.from('stanze').select('id,nome').order('nome');
    if (stanzeError) {
        window.alert(`Caricamento stanze fallito: ${stanzeError.message}`);
        return;
    }

    selStanza.innerHTML = '<option value="">Seleziona Stanza</option>' +
        (stanze || []).map(s => `<option value="${s.id}">${escapeHtml(s.nome)}</option>`).join('');
    refreshSummary();

    if (inputFoto && preview) {
        inputFoto.onchange = () => {
            const file = inputFoto.files?.[0];
            if (!file) {
                preview.src = '';
                preview.style.display = 'none';
                setImageApproval(false, 'Foto non acquisita.');
                return;
            }

            preview.src = URL.createObjectURL(file);
            preview.style.display = 'block';
            setImageApproval(true, 'Foto acquisita.');
        };
    }

    selStanza.onchange = async () => {
        await loadMobiliByStanza(selStanza.value);
        refreshSummary();
    };

    if (selScatola) selScatola.onchange = refreshSummary;
    if (selMobile) selMobile.onchange = refreshSummary;

    if (btnCreateScatola) {
        btnCreateScatola.onclick = async () => {
            if (isCreatingScatola) return;
            const stanzaId = selStanza?.value;
            const mobileId = selMobile?.value ? Number(selMobile.value) : null;

            if (!stanzaId) {
                window.alert('Per creare una scatola devi prima selezionare la stanza al passo 2.');
                goToStep(2);
                return;
            }

            const confirmCreate = window.confirm('Confermi la creazione di una nuova scatola?');
            if (!confirmCreate) return;

            try {
                isCreatingScatola = true;
                btnCreateScatola.disabled = true;
                const newScatolaId = await createNewScatola(stanzaId, mobileId);
                if (!newScatolaId) return;

                await loadScatoleAll();
                if (selScatola) {
                    const newId = String(newScatolaId);
                    const optionExists = Array.from(selScatola.options).some((opt) => opt.value === newId);

                    if (!optionExists) {
                        const singleRes = await supabase
                            .from('scatole')
                            .select('id,nome')
                            .eq('id', newScatolaId)
                            .single();

                        const label = !singleRes.error && singleRes.data
                            ? (singleRes.data.nome || String(singleRes.data.id))
                            : newId;

                        const opt = document.createElement('option');
                        opt.value = newId;
                        opt.textContent = label;
                        selScatola.insertBefore(opt, selScatola.firstChild?.nextSibling || null);
                    }

                    selScatola.value = newId;
                }

                saveLastScatolaForStanza(stanzaId, newScatolaId);
                refreshSummary();

                if (scatolaModeMsg) {
                    scatolaModeMsg.textContent = `Nuova scatola creata e selezionata: ${newScatolaId}.`;
                    scatolaModeMsg.style.color = '#047857';
                }
            } finally {
                isCreatingScatola = false;
                btnCreateScatola.disabled = false;
            }
        };
    }

    if (btnPrevStep) btnPrevStep.onclick = () => goToStep(currentStep - 1);
    if (btnNextStep) {
        btnNextStep.onclick = () => {
            if (!validateCurrentStep()) return;
            goToStep(currentStep + 1);
        };
    }

    if (form) {
        form.onsubmit = async (e) => {
            e.preventDefault();
            if (isSavingOggetto) return;

            if (currentStep !== 4) {
                window.alert('Vai al passo 4 per completare e salvare.');
                goToStep(4);
                return;
            }

            const stanzaId = selStanza?.value;
            if (!stanzaId) {
                window.alert('Seleziona la stanza prima di salvare.');
                return;
            }

            if (!inputFoto?.files?.[0]) {
                window.alert('Scatta o carica una foto prima del salvataggio.');
                return;
            }

            const scatolaIdToUse = Number(selScatola?.value || 0);
            if (!scatolaIdToUse) {
                window.alert('Seleziona una scatola dal menu dropdown.');
                return;
            }

            const scatolaCheck = await supabase
                .from('scatole')
                .select('id')
                .eq('id', scatolaIdToUse)
                .single();

            if (scatolaCheck.error || !scatolaCheck.data) {
                window.alert(`Controllo scatola fallito: ${scatolaCheck.error?.message || 'scatola non trovata'}`);
                return;
            }

            const scatolaLabel = selScatola.options[selScatola.selectedIndex]?.textContent?.trim() || String(scatolaIdToUse);

            saveLastScatolaForStanza(stanzaId, scatolaIdToUse);

            const stanzaLabel = selStanza.options[selStanza.selectedIndex]?.textContent?.trim() || '-';
            const mobileLabel = selMobile?.value
                ? (selMobile.options[selMobile.selectedIndex]?.textContent?.trim() || 'Nessuno')
                : 'Nessuno';
            const nomeValue = (document.getElementById('nomeOggetto')?.value || '').trim();
            const noteValue = (noteOggetto?.value || '').trim();

            let saved = null;
            try {
                isSavingOggetto = true;
                if (btnSave) btnSave.disabled = true;
                if (btnNextStep) btnNextStep.disabled = true;

                saved = await salvaOggetto({
                    confirmedScatolaId: scatolaIdToUse,
                    stanzaId,
                    mobileId: selMobile?.value ? Number(selMobile.value) : null,
                    note: noteValue,
                });
            } finally {
                isSavingOggetto = false;
                if (btnSave) btnSave.disabled = false;
                if (btnNextStep) btnNextStep.disabled = false;
            }

            if (!saved) return;

            lastSavedAcquisition = {
                id: saved.id,
                pathfoto: saved.pathfoto,
                nome: nomeValue || 'Senza nome',
                note: noteValue || '-',
                scatolaLabel,
                stanzaLabel,
                mobileLabel,
            };

            if (acqResultNome) acqResultNome.textContent = lastSavedAcquisition.nome;
            if (acqResultScatola) acqResultScatola.textContent = lastSavedAcquisition.scatolaLabel;
            if (acqResultStanza) acqResultStanza.textContent = lastSavedAcquisition.stanzaLabel;
            if (acqResultMobile) acqResultMobile.textContent = lastSavedAcquisition.mobileLabel;
            if (acqResultNote) acqResultNote.textContent = lastSavedAcquisition.note;
            if (acqResultImage) {
                acqResultImage.src = getPublicStorageUrl(lastSavedAcquisition.pathfoto);
                acqResultImage.style.display = 'block';
            }

            showResultPanel(true);
        };
    }

    if (btnNewAcquisition) {
        btnNewAcquisition.onclick = async () => {
            lastSavedAcquisition = null;
            await resetWizardForNewAcquisition();
        };
    }

    if (btnDeleteAcquisition) {
        btnDeleteAcquisition.onclick = async () => {
            if (isDeletingAcquisition) return;
            if (!lastSavedAcquisition?.id) {
                await resetWizardForNewAcquisition();
                return;
            }

            const confirmMsg = [
                'Confermi eliminazione del record acquisito?',
                `ID record: ${lastSavedAcquisition.id}`,
                `Oggetto: ${lastSavedAcquisition.nome || 'Senza nome'}`,
            ].join('\n');
            const ok = window.confirm(confirmMsg);
            if (!ok) return;

            try {
                isDeletingAcquisition = true;
                btnDeleteAcquisition.disabled = true;

                const { error } = await supabase.from('oggetti').delete().eq('id', lastSavedAcquisition.id);
                if (error) {
                    window.alert(`Eliminazione record fallita: ${error.message}`);
                    return;
                }

                if (lastSavedAcquisition.pathfoto) {
                    const removeRes = await removeWithBucketFallback(lastSavedAcquisition.pathfoto);
                    if (removeRes.error) {
                        const bucketInfo = removeRes.attemptedBuckets?.length
                            ? ` (bucket provati: ${removeRes.attemptedBuckets.join(', ')})`
                            : '';
                        window.alert(`Record eliminato, ma foto non rimossa: ${removeRes.error.message}${bucketInfo}`);
                    }
                }

                window.alert('Acquisizione eliminata.');
                lastSavedAcquisition = null;
                await resetWizardForNewAcquisition();
            } finally {
                isDeletingAcquisition = false;
                btnDeleteAcquisition.disabled = false;
            }
        };
    }

    setImageApproval(false, 'Foto non acquisita.');
    if (scatolaModeMsg) {
        scatolaModeMsg.textContent = 'Seleziona una scatola esistente (puo contenere oggetti di stanze diverse). Per crearne una nuova, scegli prima la stanza al passo 2.';
        scatolaModeMsg.style.color = '#334155';
    }

    await Promise.all([
        loadScatoleAll(),
        loadMobiliByStanza(''),
    ]);

    showResultPanel(false);
    renderWizardStep();
}

// --- LOGICA GESTIONE (RICERCA) ---
async function initGestione() {
    const searchInp = document.getElementById('searchInp');
    const griglia = document.getElementById('grigliaOggetti');
    const filterScatola = document.getElementById('filterGestioneScatola');
    const filterStanza = document.getElementById('filterGestioneStanza');
    const filterMobile = document.getElementById('filterGestioneMobile');
    const btnEditSelected = document.getElementById('btnGestioneEditSelected');
    const btnDeleteSelected = document.getElementById('btnGestioneDeleteSelected');
    const modalOverlay = document.getElementById('gestioneModalOverlay');
    const closeModalBtn = document.getElementById('closeGestioneModal');
    const editForm = document.getElementById('gestioneEditForm');
    const deleteBtn = document.getElementById('deleteGestioneOggetto');
    const editId = document.getElementById('gestioneOggettoId');
    const editNome = document.getElementById('gestioneOggettoNome');
    const editScatola = document.getElementById('gestioneOggettoScatola');
    const editStanza = document.getElementById('gestioneOggettoStanza');
    const editMobile = document.getElementById('gestioneOggettoMobile');
    const editNote = document.getElementById('gestioneOggettoNote');
    const previewImg = document.getElementById('gestionePreviewImg');
    const previewMeta = document.getElementById('gestionePreviewMeta');

    if (!searchInp || !griglia || !filterScatola || !filterStanza || !filterMobile || !modalOverlay || !editForm || !btnEditSelected || !btnDeleteSelected) return;

    const gestioneState = {
        rows: [],
        scatole: [],
        stanze: [],
        mobili: [],
        selectedRowId: null,
        filters: {
            term: '',
            scatola: '',
            stanza: '',
            mobile: '',
        },
    };
    const incomingPrefill = consumeGestionePrefill();

    const closeModal = () => {
        modalOverlay.hidden = true;
        editForm.reset();
        if (editId) editId.value = '';
        if (previewImg) {
            previewImg.src = '';
            previewImg.hidden = true;
        }
        if (previewMeta) previewMeta.textContent = '';
    };

    const getSelectedRow = () => gestioneState.rows.find((item) => String(item.id) === String(gestioneState.selectedRowId || '')) || null;

    const syncSelectedButtonsState = () => {
        const hasSelection = Boolean(getSelectedRow());
        btnEditSelected.disabled = !hasSelection;
        btnDeleteSelected.disabled = !hasSelection;
    };

    const buildLookupOptions = (selectEl, items, labelKey = 'nome', valueKey = 'id', emptyLabel = 'Seleziona') => {
        if (!selectEl) return;
        selectEl.innerHTML = `<option value="">${escapeHtml(emptyLabel)}</option>` +
            (items || []).map((item) => `<option value="${item[valueKey]}">${escapeHtml(item[labelKey] || String(item[valueKey]))}</option>`).join('');
    };

    const populateEditMobileOptions = (stanzaId, selectedMobileId = '') => {
        if (!editMobile) return;
        const filteredMobili = (gestioneState.mobili || []).filter((mobile) => {
            if (!stanzaId) return true;
            return String(mobile.idstanza || '') === String(stanzaId);
        });

        buildLookupOptions(editMobile, filteredMobili, 'nome', 'id', 'Nessuno');
        const selected = selectedMobileId ? String(selectedMobileId) : '';
        const exists = filteredMobili.some((mobile) => String(mobile.id) === selected);
        editMobile.value = exists ? selected : '';
    };

    const uniqueSortedValues = (rows, key) => Array.from(new Set(rows.map((row) => row[key]).filter(Boolean)))
        .sort((a, b) => a.localeCompare(b, 'it'));

    const buildFilterOptions = (selectEl, placeholder, values) => {
        selectEl.innerHTML = `<option value="">${placeholder}</option>` +
            values.map((value) => `<option value="${escapeHtml(value)}">${escapeHtml(value)}</option>`).join('');
    };

    const populateFilters = () => {
        const rowsForTerm = gestioneState.rows.filter((row) => {
            const term = gestioneState.filters.term.trim().toLowerCase();
            if (!term) return true;
            const nome = (row.oggetto_nome || '').toLowerCase();
            const note = (row.oggetto_note || '').toLowerCase();
            return nome.includes(term) || note.includes(term);
        });

        const scatole = uniqueSortedValues(rowsForTerm, 'scatola_nome');
        if (gestioneState.filters.scatola && !scatole.includes(gestioneState.filters.scatola)) {
            gestioneState.filters.scatola = '';
        }

        const rowsForStanze = rowsForTerm.filter((row) => !gestioneState.filters.scatola || row.scatola_nome === gestioneState.filters.scatola);
        const stanze = uniqueSortedValues(rowsForStanze, 'stanza_nome');
        if (gestioneState.filters.stanza && !stanze.includes(gestioneState.filters.stanza)) {
            gestioneState.filters.stanza = '';
        }

        const rowsForMobili = rowsForStanze.filter((row) => !gestioneState.filters.stanza || row.stanza_nome === gestioneState.filters.stanza);
        const mobili = uniqueSortedValues(rowsForMobili, 'mobile_nome');
        if (gestioneState.filters.mobile && !mobili.includes(gestioneState.filters.mobile)) {
            gestioneState.filters.mobile = '';
        }

        buildFilterOptions(filterScatola, 'Tutte le scatole', scatole);
        buildFilterOptions(filterStanza, 'Tutte le stanze', stanze);
        buildFilterOptions(filterMobile, 'Tutti i mobili', mobili);

        filterScatola.value = gestioneState.filters.scatola || '';
        filterStanza.value = gestioneState.filters.stanza || '';
        filterMobile.value = gestioneState.filters.mobile || '';
    };

    const getFilteredRows = () => {
        return gestioneState.rows.filter((row) => {
            const term = gestioneState.filters.term.trim().toLowerCase();
            if (term) {
                const nome = (row.oggetto_nome || '').toLowerCase();
                const note = (row.oggetto_note || '').toLowerCase();
                if (!nome.includes(term) && !note.includes(term)) return false;
            }

            if (gestioneState.filters.scatola && row.scatola_nome !== gestioneState.filters.scatola) return false;
            if (gestioneState.filters.stanza && row.stanza_nome !== gestioneState.filters.stanza) return false;
            if (gestioneState.filters.mobile && row.mobile_nome !== gestioneState.filters.mobile) return false;
            return true;
        });
    };

    const render = () => {
        const data = getFilteredRows();
        const visibleIds = new Set(data.map((row) => String(row.id)));
        if (gestioneState.selectedRowId && !visibleIds.has(String(gestioneState.selectedRowId))) {
            gestioneState.selectedRowId = null;
        }
        syncSelectedButtonsState();

        if (data.length === 0) {
            griglia.innerHTML = '<p style="grid-column:1/-1; margin:0; color:#64748b;">Nessuna immagine trovata con i filtri selezionati.</p>';
            return;
        }

        griglia.innerHTML = data.map((o) => {
            const selectedClass = String(gestioneState.selectedRowId || '') === String(o.id) ? 'is-selected' : '';
            return `
                <button type="button" class="item-card ${selectedClass}" data-row-id="${o.id}" title="Scatola ${escapeHtml(o.scatola_nome)} - ${escapeHtml(o.stanza_nome)} - ${escapeHtml(o.mobile_nome || 'Senza mobile')}">
                    <img src="${escapeHtml(getPublicStorageUrl(o.oggetto_foto))}" alt="${escapeHtml(o.oggetto_nome || 'Oggetto')}">
                </button>
            `;
        }).join('');
    };

    const fetchGestioneRows = async () => {
        const [oggettiRes, scatoleRes, stanzeRes, mobiliRes] = await Promise.all([
            supabase.from('oggetti').select('id,nome,idscatola,idstanza,idmobile,pathfoto,note').order('id', { ascending: false }),
            supabase.from('scatole').select('id,nome').order('id', { ascending: true }),
            supabase.from('stanze').select('id,nome').order('nome', { ascending: true }),
            supabase.from('mobili').select('id,nome,idstanza').order('nome', { ascending: true }),
        ]);

        const firstError = oggettiRes.error || scatoleRes.error || stanzeRes.error || mobiliRes.error;
        if (firstError) throw firstError;

        const scatolaById = new Map((scatoleRes.data || []).map((row) => [String(row.id), row.nome || String(row.id)]));
        const stanzaById = new Map((stanzeRes.data || []).map((row) => [String(row.id), row.nome || '-']));
        const mobileById = new Map((mobiliRes.data || []).map((row) => [String(row.id), row.nome || '-']));

        gestioneState.scatole = scatoleRes.data || [];
        gestioneState.stanze = stanzeRes.data || [];
        gestioneState.mobili = mobiliRes.data || [];

        gestioneState.rows = (oggettiRes.data || []).map((o) => ({
            id: o.id,
            oggetto_nome: o.nome || '',
            oggetto_note: o.note || '',
            oggetto_foto: o.pathfoto,
            idscatola: o.idscatola,
            idstanza: o.idstanza,
            idmobile: o.idmobile,
            scatola_nome: scatolaById.get(String(o.idscatola)) || String(o.idscatola || '-'),
            stanza_nome: stanzaById.get(String(o.idstanza)) || String(o.idstanza || '-'),
            mobile_nome: o.idmobile ? (mobileById.get(String(o.idmobile)) || String(o.idmobile)) : '',
        }));
    };

    const refresh = async () => {
        await fetchGestioneRows();
        populateFilters();
        render();
    };

    const openEditModal = (row) => {
        if (!row) return;

        buildLookupOptions(editScatola, gestioneState.scatole, 'nome', 'id', 'Seleziona scatola');
        buildLookupOptions(editStanza, gestioneState.stanze, 'nome', 'id', 'Seleziona stanza');

        editId.value = String(row.id);
        editNome.value = row.oggetto_nome || '';
        editScatola.value = String(row.idscatola || '');
        editStanza.value = String(row.idstanza || '');
        populateEditMobileOptions(row.idstanza, row.idmobile ? String(row.idmobile) : '');
        editNote.value = row.oggetto_note || '';

        if (previewImg) {
            if (row.oggetto_foto) {
                previewImg.src = getPublicStorageUrl(row.oggetto_foto);
                previewImg.hidden = false;
            } else {
                previewImg.src = '';
                previewImg.hidden = true;
            }
        }

        if (previewMeta) {
            previewMeta.textContent = `Scatola: ${row.scatola_nome || '-'} | Stanza: ${row.stanza_nome || '-'} | Mobile: ${row.mobile_nome || '-'}`;
        }

        modalOverlay.hidden = false;
    };

    searchInp.oninput = (e) => {
        gestioneState.filters.term = e.target.value || '';
        populateFilters();
        render();
    };

    filterScatola.onchange = () => {
        gestioneState.filters.scatola = filterScatola.value || '';
        gestioneState.filters.stanza = '';
        gestioneState.filters.mobile = '';
        populateFilters();
        render();
    };

    filterStanza.onchange = () => {
        gestioneState.filters.stanza = filterStanza.value || '';
        gestioneState.filters.mobile = '';
        populateFilters();
        render();
    };

    filterMobile.onchange = () => {
        gestioneState.filters.mobile = filterMobile.value || '';
        populateFilters();
        render();
    };

    const deleteRowById = async (row) => {
        if (!row) return;

        const ok = window.confirm('Confermi l\'eliminazione dell\'oggetto selezionato?');
        if (!ok) return;

        const { error } = await supabase.from('oggetti').delete().eq('id', row.id);
        if (error) {
            window.alert(`Eliminazione fallita: ${error.message}`);
            return;
        }

        if (row.oggetto_foto) {
            await removeWithBucketFallback(row.oggetto_foto);
        }

        closeModal();
        gestioneState.selectedRowId = null;
        syncSelectedButtonsState();
        await refresh();
    };

    griglia.onclick = (event) => {
        const card = event.target.closest('[data-row-id]');
        if (!card) return;

        const row = gestioneState.rows.find((item) => String(item.id) === String(card.dataset.rowId));
        if (!row) return;

        gestioneState.selectedRowId = row.id;
        // Click imposta la scatola e azzera i filtri a valle (stanza/mobile).
        gestioneState.filters.scatola = row.scatola_nome || '';
        gestioneState.filters.stanza = '';
        gestioneState.filters.mobile = '';

        populateFilters();
        render();
    };

    btnEditSelected.onclick = () => {
        const row = getSelectedRow();
        if (!row) return;
        openEditModal(row);
    };

    btnDeleteSelected.onclick = async () => {
        const row = getSelectedRow();
        if (!row) return;
        await deleteRowById(row);
    };

    if (closeModalBtn) closeModalBtn.onclick = closeModal;
    modalOverlay.onclick = (event) => {
        if (event.target === modalOverlay) closeModal();
    };

    editForm.onsubmit = async (event) => {
        event.preventDefault();
        const id = editId.value;
        if (!id) return;

        const payload = {
            nome: normalizeText(editNome.value),
            idscatola: Number(editScatola.value),
            idstanza: Number(editStanza.value),
            idmobile: editMobile.value ? Number(editMobile.value) : null,
            note: normalizeText(editNote.value),
        };

        if (!payload.idscatola || !payload.idstanza) {
            window.alert('Scatola e stanza sono obbligatorie.');
            return;
        }

        const ok = window.confirm('Confermi la modifica dei dati dell\'oggetto?');
        if (!ok) return;

        const { error } = await supabase.from('oggetti').update(payload).eq('id', id);
        if (error) {
            window.alert(`Modifica fallita: ${error.message}`);
            return;
        }

        closeModal();
        await refresh();
    };

    if (deleteBtn) {
        deleteBtn.onclick = async () => {
            const id = editId.value;
            if (!id) return;
            const row = gestioneState.rows.find((item) => String(item.id) === String(id));
            await deleteRowById(row);
        };
    }

    if (editStanza) {
        editStanza.onchange = () => {
            populateEditMobileOptions(editStanza.value || '', '');
        };
    }

    try {
        await refresh();
        if (incomingPrefill?.scatola) {
            gestioneState.filters.scatola = String(incomingPrefill.scatola);
            gestioneState.filters.stanza = '';
            gestioneState.filters.mobile = '';
            populateFilters();
            render();
        }
        syncSelectedButtonsState();
    } catch (error) {
        griglia.innerHTML = `<p style="grid-column:1/-1; color:#b91c1c;">Errore caricamento dati: ${escapeHtml(error.message || String(error))}</p>`;
    }
}

const serviziState = {
    stanze: [],
    mobili: [],
    scatole: [],
    selectedStanzaId: null,
    scatoleMobiliByScatola: new Map(),
    scatolaStanzeByScatola: new Map(),
    oggettiCountByScatola: new Map(),
    stanzaCountByScatola: new Map(),
    filters: {
        scatolaNome: '',
        stanza: '',
    },
    scatolaFotoFile: null,
    scatolaFotoRemoved: false,
};

function showServiziMsg(message, type = 'ok') {
    const msgEl = document.getElementById('serviziMsg');
    if (!msgEl) return;
    msgEl.className = `servizi-msg ${type}`;
    msgEl.textContent = message;
}

function normalizeText(value) {
    return value?.trim() || null;
}

function getPublicStorageUrl(path) {
    if (!path) return '';
    return supabase.storage.from(storageState.bucket).getPublicUrl(path).data.publicUrl;
}

function escapeHtml(value) {
    return String(value || '')
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#039;');
}

function getScatolaLinkedStanzeLabel(scatolaId) {
    return serviziState.scatolaStanzeByScatola.get(String(scatolaId)) || '-';
}

function getScatolaLinkedMobiliLabel(scatolaId) {
    return serviziState.scatoleMobiliByScatola.get(String(scatolaId)) || '-';
}

function getScatolaDisplayName(scatola) {
    return normalizeText(scatola?.nome) || String(scatola?.id || '');
}

function getScatolaLabelData(scatola) {
        const scatolaKey = String(scatola?.id || '');
        const rawNome = normalizeText(scatola?.nome) || String(scatola?.id || '-');
        const qty = serviziState.oggettiCountByScatola.get(scatolaKey) || 0;
        const stanzaMap = serviziState.stanzaCountByScatola.get(scatolaKey) || new Map();

        const sortedStanze = Array.from(stanzaMap.entries())
                .map(([nome, count]) => ({ nome, count }))
                .sort((a, b) => (b.count - a.count) || a.nome.localeCompare(b.nome, 'it'));

        const primaryStanza = sortedStanze[0] || null;
        const otherStanze = sortedStanze.slice(1);

        return {
                scatolaNome: rawNome,
                quantitaOggetti: qty,
                primaryStanza,
                otherStanze,
        };
}

function askFragileLabelChoice() {
    return new Promise((resolve) => {
        const overlay = document.createElement('div');
        overlay.style.position = 'fixed';
        overlay.style.inset = '0';
        overlay.style.background = 'rgba(0,0,0,0.45)';
        overlay.style.display = 'flex';
        overlay.style.alignItems = 'center';
        overlay.style.justifyContent = 'center';
        overlay.style.zIndex = '2000';

        const box = document.createElement('div');
        box.style.background = '#fff';
        box.style.border = '1px solid #d6d3d1';
        box.style.borderRadius = '12px';
        box.style.padding = '14px';
        box.style.width = 'min(420px, calc(100% - 28px))';
        box.style.boxSizing = 'border-box';

        const title = document.createElement('h4');
        title.textContent = 'Etichetta FRAGILE';
        title.style.margin = '0 0 8px';

        const msg = document.createElement('p');
        msg.textContent = 'Apporre la scritta FRAGILE in rosso sull\'etichetta?';
        msg.style.margin = '0 0 12px';

        const actions = document.createElement('div');
        actions.style.display = 'flex';
        actions.style.justifyContent = 'flex-end';
        actions.style.gap = '8px';

        const btnAnnulla = document.createElement('button');
        btnAnnulla.type = 'button';
        btnAnnulla.textContent = 'Annulla';
        btnAnnulla.className = 'mini-btn';

        const btnNo = document.createElement('button');
        btnNo.type = 'button';
        btnNo.textContent = 'No';
        btnNo.className = 'mini-btn';

        const btnSi = document.createElement('button');
        btnSi.type = 'button';
        btnSi.textContent = 'Si';
        btnSi.className = 'mini-btn';
        btnSi.style.background = '#0f766e';
        btnSi.style.color = '#fff';
        btnSi.style.borderColor = '#0f766e';

        const cleanup = (result) => {
            overlay.remove();
            resolve(result);
        };

        btnAnnulla.onclick = () => cleanup(null);
        btnNo.onclick = () => cleanup(false);
        btnSi.onclick = () => cleanup(true);
        overlay.onclick = (event) => {
            if (event.target === overlay) cleanup(null);
        };

        actions.append(btnAnnulla, btnNo, btnSi);
        box.append(title, msg, actions);
        overlay.appendChild(box);
        document.body.appendChild(overlay);
    });
}

function printScatolaLabelA5(scatola, includeFragile) {
    const labelData = getScatolaLabelData(scatola);
    const printWindow = window.open('', '_blank', 'width=900,height=700');
        if (!printWindow) {
                showServiziMsg('Popup bloccato: consenti le finestre per stampare l\'etichetta.', 'err');
                return;
        }

        const primaryName = labelData.primaryStanza
            ? labelData.primaryStanza.nome
            : 'Nessuna stanza collegata';
        const primaryCount = labelData.primaryStanza ? labelData.primaryStanza.count : null;
        const otherLabels = labelData.otherStanze.length
                ? labelData.otherStanze.map((item) => `${item.nome} (${item.count})`).join(' - ')
                : 'Nessuna';

        const html = `
<!doctype html>
<html lang="it">
<head>
    <meta charset="utf-8">
    <title>Etichetta Scatola ${escapeHtml(labelData.scatolaNome)}</title>
    <style>
        @page { size: A5 portrait; margin: 8mm; }
        html, body { margin: 0; padding: 0; font-family: Arial, sans-serif; }
        body { width: 100%; }
        .top-actions { display: flex; justify-content: flex-end; gap: 8px; padding: 8px; }
        .top-actions button { border: 1px solid #d6d3d1; border-radius: 8px; padding: 7px 12px; cursor: pointer; font-weight: 700; background: #fff; }
        .top-actions .primary { background: #0f766e; color: #fff; border-color: #0f766e; }
        .label { padding: 14px; min-height: calc(210mm - 16mm); box-sizing: border-box; }
        .top-grid { display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); gap: 16px; align-items: start; }
        .top-grid > div { min-width: 0; }
        .row-first { margin-bottom: 18px; }
        .row { margin-bottom: 10px; }
        .title { font-size: 13px; text-transform: uppercase; letter-spacing: 0.4px; color: #374151; margin-bottom: 4px; }
        .value-big { font-size: 44px; font-weight: 900; line-height: 1.02; color: #111827; white-space: normal; overflow-wrap: anywhere; word-break: break-word; }
        .value-medium { font-size: 44px; font-weight: 800; line-height: 1.05; color: #111827; white-space: normal; overflow-wrap: anywhere; word-break: break-word; }
        .primary-count { font-size: 0.5em; font-weight: 700; color: #374151; margin-left: 6px; }
        .value-small { font-size: 16px; color: #374151; }
        .qty { font-size: 28px; font-weight: 800; }
        .fragile { margin-top: 14px; font-size: 38px; font-weight: 900; color: #dc2626; border: 3px solid #dc2626; border-radius: 8px; text-align: center; padding: 8px; }
        @media print {
            .screen-only { display: none !important; }
            html, body { width: 148mm; height: 210mm; }
            .label { min-height: calc(210mm - 16mm); }
        }
    </style>
</head>
<body>
    <div class="top-actions screen-only">
        <button type="button" onclick="window.close()">Chiudi</button>
        <button type="button" class="primary" onclick="window.print()">Stampa</button>
    </div>
    <article class="label">
        <section class="top-grid row row-first">
            <div>
                <div class="title">Scatola</div>
                <div class="value-big">${escapeHtml(labelData.scatolaNome)}</div>
            </div>
            <div>
                <div class="title">Stanza</div>
                <div class="value-medium">
                    ${escapeHtml(primaryName)}
                    ${primaryCount !== null ? `<span class="primary-count">(${escapeHtml(String(primaryCount))})</span>` : ''}
                </div>
            </div>
        </section>
        <section class="row">
            <div class="title">Altre stanze</div>
            <div class="value-small">${escapeHtml(otherLabels)}</div>
        </section>
        <section class="row">
            <div class="title">Quantita oggetti</div>
            <div class="qty">${labelData.quantitaOggetti}</div>
        </section>
        ${includeFragile ? '<section class="fragile">FRAGILE</section>' : ''}
    </article>
</body>
</html>`;

        printWindow.document.open();
        printWindow.document.write(html);
        printWindow.document.close();
        printWindow.focus();
}

async function initServizi(mode = 'stanze') {
    const isStanzeMode = mode === 'stanze';
    const isScatoleMode = mode === 'scatole';
    const panel = document.querySelector('.servizi-panel');
    const btnViewStanze = document.getElementById('btnViewStanze');
    const btnViewScatole = document.getElementById('btnViewScatole');
    const viewStanze = document.getElementById('viewStanze');
    const viewScatole = document.getElementById('viewScatole');
    const openStanzaModal = document.getElementById('openStanzaModal');
    const editSelectedStanza = document.getElementById('editSelectedStanza');
    const deleteSelectedStanza = document.getElementById('deleteSelectedStanza');
    const openMobileModal = document.getElementById('openMobileModal');
    const openScatolaModal = document.getElementById('openScatolaModal');
    const modalOverlay = document.getElementById('serviziModalOverlay');
    const modalStanze = document.getElementById('modalStanze');
    const modalMobili = document.getElementById('modalMobili');
    const modalScatole = document.getElementById('modalScatole');
    const btnExpandMobiliTree = document.getElementById('btnExpandMobiliTree');
    const btnCollapseMobiliTree = document.getElementById('btnCollapseMobiliTree');
    const stanzaFocusSummary = document.getElementById('stanzaFocusSummary');
    const stanzaFocusList = document.getElementById('stanzaFocusList');
    const formStanze = document.getElementById('formStanze');
    const formMobili = document.getElementById('formMobili');
    const formScatole = document.getElementById('formScatole');
    const resetStanza = document.getElementById('resetStanza');
    const resetMobile = document.getElementById('resetMobile');
    const resetScatola = document.getElementById('resetScatola');
    const scatolaStanza = document.getElementById('scatolaStanza');
    const scatolaMobile = document.getElementById('scatolaMobile');
    const filterScatolaNome = document.getElementById('filterScatolaNome');
    const filterScatolaStanza = document.getElementById('filterScatolaStanza');
    const scatolaNomeField = document.getElementById('scatolaNomeField');
    const scatolaNomeInput = document.getElementById('scatolaNome');
    const scatolaFotoFile = document.getElementById('scatolaFotoFile');
    const btnScatolaFoto = document.getElementById('btnScatolaFoto');
    const btnRimuoviScatolaFoto = document.getElementById('btnRimuoviScatolaFoto');
    const modalTitleScatole = document.getElementById('modalTitleScatole');
    const submitScatola = document.getElementById('submitScatola');

    if (isStanzeMode && (!formStanze || !formMobili)) return;
    if (isScatoleMode && !formScatole) return;

    const setServiziView = (viewName) => {
        if (isStanzeMode && viewName !== 'stanze') return;
        if (isScatoleMode && viewName !== 'scatole') return;

        if (!viewStanze && !viewScatole && !btnViewStanze && !btnViewScatole) return;

        const views = [
            { key: 'stanze', el: viewStanze, btn: btnViewStanze },
            { key: 'scatole', el: viewScatole, btn: btnViewScatole },
        ];

        views.forEach((view) => {
            const isActive = view.key === viewName;
            if (view.el) view.el.classList.toggle('is-hidden', !isActive);
            if (view.btn) {
                view.btn.classList.toggle('active', isActive);
                view.btn.setAttribute('aria-selected', String(isActive));
            }
        });
    };

    const closeAllModals = () => {
        if (modalOverlay) {
            modalOverlay.classList.add('is-hidden');
            modalOverlay.setAttribute('aria-hidden', 'true');
        }

        [modalStanze, modalMobili, modalScatole].forEach((modal) => {
            modal?.classList.add('is-hidden');
        });
    };

    const openModal = (name) => {
        closeAllModals();
        if (!modalOverlay) return;

        const map = {
            stanze: modalStanze,
            mobili: modalMobili,
            scatole: modalScatole,
        };

        const target = map[name];
        if (!target) return;

        modalOverlay.classList.remove('is-hidden');
        modalOverlay.setAttribute('aria-hidden', 'false');
        target.classList.remove('is-hidden');
    };

    const renderStanzaFocusPanel = () => {
        if (!stanzaFocusSummary || !stanzaFocusList) return;

        const selectedId = serviziState.selectedStanzaId ? String(serviziState.selectedStanzaId) : null;
        const stanza = selectedId
            ? serviziState.stanze.find((item) => String(item.id) === selectedId)
            : null;

        if (!stanza) {
            stanzaFocusSummary.textContent = "Seleziona una stanza dall'albero per vedere e gestire i mobili collegati.";
            stanzaFocusList.innerHTML = '<div class="tree-empty">Nessuna stanza selezionata.</div>';
            return;
        }

        const linkedMobili = serviziState.mobili.filter((mobile) => String(mobile.idstanza) === String(stanza.id));
        stanzaFocusSummary.textContent = `Stanza: ${stanza.nome} - ${linkedMobili.length} mobili collegati.`;

        const createButton = `<button class="mini-btn" data-action="new-mobile-for-stanza" data-id="${stanza.id}">+ Aggiungi mobile</button>`;
        if (linkedMobili.length === 0) {
            stanzaFocusList.innerHTML = `
                <div class="tree-empty">Questa stanza non ha mobili collegati.</div>
                <div class="azioni">${createButton}</div>
            `;
            return;
        }

        stanzaFocusList.innerHTML = `
            <div class="azioni">${createButton}</div>
            ${linkedMobili.map((mobile) => `
                <article class="stanza-focus-item">
                    <div class="stanza-focus-item-top">
                        <strong>${escapeHtml(mobile.nome)}</strong>
                        <div>
                            <button class="mini-btn" data-action="edit-mobile" data-id="${mobile.id}">Modifica</button>
                            <button class="mini-btn" data-action="delete-mobile" data-id="${mobile.id}">Elimina</button>
                        </div>
                    </div>
                    <p class="stanza-focus-note">${escapeHtml(mobile.note || 'Nessuna nota')}</p>
                </article>
            `).join('')}
        `;
    };

    const renderServiziTables = () => {
        const tbScatole = document.getElementById('tbScatole');
        const mobiliTreeByStanza = document.getElementById('mobiliTreeByStanza');
        const mobiliTreeSummary = document.getElementById('mobiliTreeSummary');

        if (mobiliTreeByStanza) {
            const mobiliByStanza = new Map();
            serviziState.stanze.forEach((stanza) => {
                mobiliByStanza.set(String(stanza.id), []);
            });

            serviziState.mobili.forEach((mobile) => {
                const key = String(mobile.idstanza || 'orphan');
                if (!mobiliByStanza.has(key)) mobiliByStanza.set(key, []);
                mobiliByStanza.get(key).push(mobile);
            });

            const treeByStanza = serviziState.stanze.map((stanza) => {
                const items = mobiliByStanza.get(String(stanza.id)) || [];
                const isSelected = String(serviziState.selectedStanzaId || '') === String(stanza.id);
                const openAttr = isSelected ? 'open' : '';
                const itemsHtml = items.length
                    ? `<div class="tree-items">${items.map((mobile) => `
                        <article class="tree-item">
                            <div class="tree-item-top">
                                <strong>${escapeHtml(mobile.nome)}</strong>
                                <div>
                                    <button class="mini-btn" data-action="edit-mobile" data-id="${mobile.id}">Modifica</button>
                                    <button class="mini-btn" data-action="delete-mobile" data-id="${mobile.id}">Elimina</button>
                                </div>
                            </div>
                            <p class="tree-item-note">${escapeHtml(mobile.note || 'Nessuna nota')}</p>
                        </article>
                    `).join('')}</div>`
                    : '<div class="tree-empty">Nessun mobile in questa stanza</div>';

                const stanzaMeta = stanza.note
                    ? `<p class="tree-stanza-meta">${escapeHtml(stanza.note)}</p>`
                    : '';

                const stanzaActions = `
                    <div class="tree-stanza-actions">
                        <button class="mini-btn" data-action="new-mobile-for-stanza" data-id="${stanza.id}">+ Mobile</button>
                        <button class="mini-btn" data-action="edit-stanza" data-id="${stanza.id}">Modifica Stanza</button>
                        <button class="mini-btn" data-action="delete-stanza" data-id="${stanza.id}">Elimina Stanza</button>
                    </div>
                `;

                return `
                    <details class="tree-node" data-tree-node ${openAttr}>
                        <summary data-action="select-stanza" data-id="${stanza.id}" class="${isSelected ? 'is-selected' : ''}">
                            <span>${escapeHtml(stanza.nome)}</span>
                            <span class="tree-count">${items.length}</span>
                        </summary>
                        ${stanzaMeta}
                        ${stanzaActions}
                        ${itemsHtml}
                    </details>
                `;
            });

            const orphanItems = mobiliByStanza.get('orphan') || [];
            if (orphanItems.length > 0) {
                treeByStanza.push(`
                    <details class="tree-node" data-tree-node>
                        <summary>
                            <span>Assegnazione non valida</span>
                            <span class="tree-count">${orphanItems.length}</span>
                        </summary>
                        <div class="tree-items">${orphanItems.map((mobile) => `
                            <article class="tree-item">
                                <div class="tree-item-top">
                                    <strong>${escapeHtml(mobile.nome)}</strong>
                                    <div>
                                        <button class="mini-btn" data-action="edit-mobile" data-id="${mobile.id}">Modifica</button>
                                        <button class="mini-btn" data-action="delete-mobile" data-id="${mobile.id}">Elimina</button>
                                    </div>
                                </div>
                                <p class="tree-item-note">${escapeHtml(mobile.note || 'Nessuna nota')}</p>
                            </article>
                        `).join('')}</div>
                    </details>
                `);
            }

            if (treeByStanza.length === 0) {
                mobiliTreeByStanza.innerHTML = '<div class="tree-empty">Nessuna stanza disponibile. Crea una nuova stanza per iniziare.</div>';
            } else {
                mobiliTreeByStanza.innerHTML = treeByStanza.join('');
            }
        }

        if (mobiliTreeSummary) {
            mobiliTreeSummary.textContent = `Vista compatta: ${serviziState.stanze.length} stanze, ${serviziState.mobili.length} mobili. Espandi una stanza per vedere i dettagli.`;
        }

        renderStanzaFocusPanel();

        if (tbScatole) {
            const filteredScatole = serviziState.scatole.filter((scatola) => {
                const matchesNome = !serviziState.filters.scatolaNome || getScatolaDisplayName(scatola) === serviziState.filters.scatolaNome;
                if (!matchesNome) return false;

                if (!serviziState.filters.stanza) return true;
                const stanzaMap = serviziState.stanzaCountByScatola.get(String(scatola.id)) || new Map();
                return stanzaMap.has(serviziState.filters.stanza);
            });

            if (filteredScatole.length === 0) {
                tbScatole.innerHTML = '<tr><td colspan="5">Nessuna scatola trovata con i filtri selezionati.</td></tr>';
                return;
            }

            tbScatole.innerHTML = filteredScatole.map(s => `
                <tr data-scatola-id="${s.id}">
                    <td>${escapeHtml(getScatolaDisplayName(s))}</td>
                    <td>${escapeHtml(getScatolaLinkedStanzeLabel(s.id))}</td>
                    <td>${escapeHtml(getScatolaLinkedMobiliLabel(s.id))}</td>
                    <td>${serviziState.oggettiCountByScatola.get(String(s.id)) || 0}</td>
                    <td>
                        <button class="mini-btn" data-action="print-scatola-label" data-id="${s.id}">Etichetta PDF</button>
                        <button class="mini-btn" data-action="view-scatola-foto" data-id="${s.id}">Foto</button>
                        <button class="mini-btn" data-action="edit-scatola" data-id="${s.id}">Modifica</button>
                        <button class="mini-btn" data-action="delete-scatola" data-id="${s.id}">Elimina</button>
                    </td>
                </tr>
            `).join('');
        }
    };

    const renderSelects = () => {
        const mobileStanza = document.getElementById('mobileStanza');

        const stanzaOptions = '<option value="">Seleziona stanza</option>' +
            serviziState.stanze.map(s => `<option value="${s.id}">${escapeHtml(s.nome)}</option>`).join('');

        if (mobileStanza) mobileStanza.innerHTML = stanzaOptions;

        if (filterScatolaNome) {
            const oldValue = serviziState.filters.scatolaNome;
            const nomi = Array.from(new Set(serviziState.scatole
                .map((s) => getScatolaDisplayName(s))
                .filter(Boolean)))
                .sort((a, b) => a.localeCompare(b, 'it'));

            filterScatolaNome.innerHTML = '<option value="">Tutti i nomi</option>' +
                nomi.map((nome) => `<option value="${escapeHtml(nome)}">${escapeHtml(nome)}</option>`).join('');

            if (oldValue && nomi.includes(oldValue)) {
                filterScatolaNome.value = oldValue;
            } else {
                filterScatolaNome.value = '';
                serviziState.filters.scatolaNome = '';
            }
        }

        if (filterScatolaStanza) {
            const oldValue = serviziState.filters.stanza;
            const stanze = Array.from(new Set(
                Array.from(serviziState.stanzaCountByScatola.values())
                    .flatMap((stanzaMap) => Array.from(stanzaMap.keys()))
                    .filter(Boolean)
            )).sort((a, b) => a.localeCompare(b, 'it'));

            filterScatolaStanza.innerHTML = '<option value="">Tutte le stanze</option>' +
                stanze.map((stanza) => `<option value="${escapeHtml(stanza)}">${escapeHtml(stanza)}</option>`).join('');

            if (oldValue && stanze.includes(oldValue)) {
                filterScatolaStanza.value = oldValue;
            } else {
                filterScatolaStanza.value = '';
                serviziState.filters.stanza = '';
            }
        }
    };

    const resetStanzaForm = () => {
        const stanzaId = document.getElementById('stanzaId');
        const stanzaNome = document.getElementById('stanzaNome');
        const stanzaNote = document.getElementById('stanzaNote');
        if (stanzaId) stanzaId.value = '';
        if (stanzaNome) stanzaNome.value = '';
        if (stanzaNote) stanzaNote.value = '';
    };

    const resetMobileForm = () => {
        const mobileId = document.getElementById('mobileId');
        const mobileNome = document.getElementById('mobileNome');
        const mobileStanza = document.getElementById('mobileStanza');
        const mobileNote = document.getElementById('mobileNote');
        if (mobileId) mobileId.value = '';
        if (mobileNome) mobileNome.value = '';
        if (mobileStanza) mobileStanza.value = '';
        if (mobileNote) mobileNote.value = '';
    };

    const resetScatolaPhotoUi = () => {
        const info = document.getElementById('scatolaFotoInfo');
        const preview = document.getElementById('scatolaFotoPreview');
        if (info) info.textContent = 'Nessuna foto selezionata';
        if (preview) {
            preview.style.display = 'none';
            preview.src = '';
        }
    };

    const resetScatolaForm = () => {
        const scatolaId = document.getElementById('scatolaId');
        const scatolaNome = document.getElementById('scatolaNome');
        const scatolaStanzaEl = document.getElementById('scatolaStanza');
        const scatolaMobileEl = document.getElementById('scatolaMobile');
        const scatolaNote = document.getElementById('scatolaNote');
        if (scatolaId) scatolaId.value = '';
        if (scatolaNome) scatolaNome.value = '';
        if (scatolaStanzaEl) scatolaStanzaEl.value = '-';
        if (scatolaMobileEl) scatolaMobileEl.value = '-';
        if (scatolaNote) scatolaNote.value = '';
        if (scatolaFotoFile) scatolaFotoFile.value = '';
        serviziState.scatolaFotoFile = null;
        serviziState.scatolaFotoRemoved = false;
        resetScatolaPhotoUi();
    };

    const openEditStanzaById = (stanzaId) => {
        const stanza = serviziState.stanze.find((item) => String(item.id) === String(stanzaId));
        if (!stanza) {
            showServiziMsg('Stanza non trovata.', 'err');
            return;
        }

        document.getElementById('stanzaId').value = stanza.id;
        document.getElementById('stanzaNome').value = stanza.nome || '';
        document.getElementById('stanzaNote').value = stanza.note || '';
        openModal('stanze');
        showServiziMsg(`Modifica stanza #${stanza.id}`, 'ok');
    };

    const deleteStanzaWithChecks = async (stanzaId) => {
        const targetId = String(stanzaId || '');
        if (!targetId) {
            showServiziMsg('Seleziona prima una stanza dall\'albero.', 'err');
            return;
        }

        const [mobiliCheckRes, oggettiCheckRes] = await Promise.all([
            supabase
                .from('mobili')
                .select('*', { head: true, count: 'exact' })
                .eq('idstanza', targetId),
            supabase
                .from('oggetti')
                .select('*', { head: true, count: 'exact' })
                .eq('idstanza', targetId),
        ]);

        const { count: mobiliCount, error: mobiliCheckError } = mobiliCheckRes;
        const { count: oggettiCount, error: oggettiCheckError } = oggettiCheckRes;

        if (mobiliCheckError) {
            showServiziMsg(`Verifica mobili collegati fallita: ${mobiliCheckError.message}`, 'err');
            return;
        }

        if (oggettiCheckError) {
            showServiziMsg(`Verifica oggetti collegati fallita: ${oggettiCheckError.message}`, 'err');
            return;
        }

        if ((mobiliCount || 0) > 0 || (oggettiCount || 0) > 0) {
            const parti = [];
            if ((mobiliCount || 0) > 0) parti.push(`${mobiliCount} mobili`);
            if ((oggettiCount || 0) > 0) parti.push(`${oggettiCount} oggetti`);
            showServiziMsg(`Impossibile eliminare la stanza: presenti collegamenti a ${parti.join(' e ')}.`, 'err');
            return;
        }

        const ok = window.confirm(`Eliminare stanza #${targetId}?`);
        if (!ok) return;

        const { error } = await supabase.from('stanze').delete().eq('id', targetId);
        if (error) {
            showServiziMsg(`Eliminazione stanza fallita: ${error.message}`, 'err');
            return;
        }

        showServiziMsg('Stanza eliminata con successo.', 'ok');
        await loadServiziData();
    };

    const deleteMobileWithChecks = async (mobileId) => {
        const targetId = String(mobileId || '');
        if (!targetId) {
            showServiziMsg('Mobile non valido per l\'eliminazione.', 'err');
            return;
        }

        const { count: oggettiCount, error: oggettiCheckError } = await supabase
            .from('oggetti')
            .select('*', { head: true, count: 'exact' })
            .eq('idmobile', targetId);

        if (oggettiCheckError) {
            showServiziMsg(`Verifica oggetti collegati al mobile fallita: ${oggettiCheckError.message}`, 'err');
            return;
        }

        if ((oggettiCount || 0) > 0) {
            showServiziMsg(`Impossibile eliminare il mobile: presenti ${oggettiCount} oggetti collegati.`, 'err');
            return;
        }

        const ok = window.confirm(`Eliminare mobile #${targetId}?`);
        if (!ok) return;

        const { error } = await supabase.from('mobili').delete().eq('id', targetId);
        if (error) {
            showServiziMsg(`Eliminazione mobile fallita: ${error.message}`, 'err');
            return;
        }

        showServiziMsg('Mobile eliminato con successo.', 'ok');
        await loadServiziData();
    };

    const deleteScatolaWithChecks = async (scatolaId) => {
        const targetId = String(scatolaId || '');
        if (!targetId) {
            showServiziMsg('Scatola non valida per l\'eliminazione.', 'err');
            return;
        }

        const { count: oggettiCount, error: oggettiCheckError } = await supabase
            .from('oggetti')
            .select('*', { head: true, count: 'exact' })
            .eq('idscatola', targetId);

        if (oggettiCheckError) {
            showServiziMsg(`Verifica oggetti collegati alla scatola fallita: ${oggettiCheckError.message}`, 'err');
            return;
        }

        if ((oggettiCount || 0) > 0) {
            showServiziMsg(`Impossibile eliminare la scatola: presenti ${oggettiCount} oggetti collegati.`, 'err');
            return;
        }

        const ok = window.confirm(`Eliminare scatola #${targetId}?`);
        if (!ok) return;

        const { error } = await supabase.from('scatole').delete().eq('id', targetId);
        if (error) {
            showServiziMsg(`Eliminazione scatola fallita: ${error.message}`, 'err');
            return;
        }

        showServiziMsg('Scatola eliminata con successo.', 'ok');
        await loadServiziData();
    };

    const setScatolaModalMode = (isEdit) => {
        if (modalTitleScatole) modalTitleScatole.textContent = isEdit ? 'Modifica Scatola' : 'Nuova Scatola';
        if (submitScatola) submitScatola.textContent = isEdit ? 'Aggiorna' : 'Salva';
        if (resetScatola) resetScatola.textContent = isEdit ? 'Annulla Modifica' : 'Annulla';
        if (scatolaNomeField) scatolaNomeField.style.display = isEdit ? '' : 'none';
        if (scatolaNomeInput) {
            scatolaNomeInput.readOnly = !isEdit;
            scatolaNomeInput.placeholder = isEdit ? 'Nome scatola' : '';
            if (!isEdit) scatolaNomeInput.value = '';
        }
    };

    const openScatolaEditor = (scatolaId, options = {}) => {
        const { fotoMode = false } = options;
        setServiziView('scatole');

        const scatola = serviziState.scatole.find((item) => String(item.id) === String(scatolaId));
        if (!scatola) {
            showServiziMsg('Scatola non trovata.', 'err');
            return null;
        }

        setScatolaModalMode(true);
        document.getElementById('scatolaId').value = scatola.id;
        document.getElementById('scatolaNome').value = scatola.nome || '';
        document.getElementById('scatolaStanza').value = getScatolaLinkedStanzeLabel(scatola.id);
        document.getElementById('scatolaMobile').value = getScatolaLinkedMobiliLabel(scatola.id);
        document.getElementById('scatolaNote').value = scatola.note || '';

        serviziState.scatolaFotoFile = null;
        serviziState.scatolaFotoRemoved = false;
        if (scatolaFotoFile) scatolaFotoFile.value = '';

        const info = document.getElementById('scatolaFotoInfo');
        const preview = document.getElementById('scatolaFotoPreview');
        if (scatola.pathfoto) {
            if (info) info.textContent = `Foto attuale: ${scatola.pathfoto}. Per sostituirla devi prima rimuoverla.`;
            if (preview) {
                preview.src = getPublicStorageUrl(scatola.pathfoto);
                preview.style.display = 'block';
            }
        } else {
            resetScatolaPhotoUi();
            if (info) info.textContent = 'Nessuna foto presente. Puoi acquisire o caricare una foto.';
        }

        openModal('scatole');
        if (fotoMode) {
            showServiziMsg(`Gestione foto scatola #${scatola.id}: acquisisci/carica o rimuovi.`, 'ok');
            document.getElementById('btnScatolaFoto')?.scrollIntoView({ block: 'center', behavior: 'smooth' });
        } else {
            showServiziMsg(`Modifica scatola #${scatola.id}`, 'ok');
        }

        return scatola;
    };

    const loadServiziData = async (options = {}) => {
        const { silent = false } = options;
        if (!silent) showServiziMsg('Caricamento dati...', 'ok');

        const [stanzeRes, mobiliRes, scatoleRes, scatoleStanzeRes, oggettiRes] = await Promise.all([
            supabase.from('stanze').select('*').order('nome', { ascending: true }),
            supabase.from('mobili').select('*').order('nome', { ascending: true }),
            supabase.from('scatole').select('*').order('id', { ascending: true }),
            supabase.from('vista_scatole_stanze').select('scatola_id,stanze_presenti'),
            supabase.from('oggetti').select('idscatola,idstanza,idmobile'),
        ]);

        if (stanzeRes.error || mobiliRes.error || scatoleRes.error || oggettiRes.error) {
            throw new Error(stanzeRes.error?.message || mobiliRes.error?.message || scatoleRes.error?.message || oggettiRes.error?.message || 'Errore caricamento dati');
        }

        serviziState.stanze = stanzeRes.data || [];
        serviziState.mobili = mobiliRes.data || [];
        serviziState.scatole = scatoleRes.data || [];
        if (serviziState.selectedStanzaId && !serviziState.stanze.some((stanza) => String(stanza.id) === String(serviziState.selectedStanzaId))) {
            serviziState.selectedStanzaId = null;
        }
        serviziState.scatoleMobiliByScatola = new Map();
        serviziState.scatolaStanzeByScatola = new Map();
        serviziState.oggettiCountByScatola = new Map();
        serviziState.stanzaCountByScatola = new Map();

        const stanzaById = new Map(serviziState.stanze.map((stanza) => [String(stanza.id), stanza.nome || String(stanza.id)]));
        const mobileById = new Map(serviziState.mobili.map((mobile) => [String(mobile.id), mobile.nome || String(mobile.id)]));
        const linkedStanzeByScatola = new Map();
        const linkedMobiliByScatola = new Map();

        (oggettiRes.data || []).forEach((oggetto) => {
            const scatolaKey = String(oggetto.idscatola || '');
            if (!scatolaKey) return;

            const previousQty = serviziState.oggettiCountByScatola.get(scatolaKey) || 0;
            serviziState.oggettiCountByScatola.set(scatolaKey, previousQty + 1);

            if (!linkedStanzeByScatola.has(scatolaKey)) linkedStanzeByScatola.set(scatolaKey, new Set());
            if (!linkedMobiliByScatola.has(scatolaKey)) linkedMobiliByScatola.set(scatolaKey, new Set());
            if (!serviziState.stanzaCountByScatola.has(scatolaKey)) serviziState.stanzaCountByScatola.set(scatolaKey, new Map());

            if (oggetto.idstanza) {
                const stanzaNome = stanzaById.get(String(oggetto.idstanza)) || String(oggetto.idstanza);
                linkedStanzeByScatola.get(scatolaKey).add(stanzaNome);
                const stanzaMap = serviziState.stanzaCountByScatola.get(scatolaKey);
                stanzaMap.set(stanzaNome, (stanzaMap.get(stanzaNome) || 0) + 1);
            }

            if (oggetto.idmobile) {
                linkedMobiliByScatola.get(scatolaKey).add(mobileById.get(String(oggetto.idmobile)) || String(oggetto.idmobile));
            }
        });

        linkedStanzeByScatola.forEach((values, scatolaKey) => {
            const labels = Array.from(values).filter(Boolean).sort((a, b) => a.localeCompare(b, 'it'));
            serviziState.scatolaStanzeByScatola.set(scatolaKey, labels.length ? labels.join(', ') : '-');
        });

        linkedMobiliByScatola.forEach((values, scatolaKey) => {
            const labels = Array.from(values).filter(Boolean).sort((a, b) => a.localeCompare(b, 'it'));
            serviziState.scatoleMobiliByScatola.set(scatolaKey, labels.length ? labels.join(', ') : '-');
        });

        if (!scatoleStanzeRes.error) {
            (scatoleStanzeRes.data || []).forEach((row) => {
                const key = String(row.scatola_id);
                if (!serviziState.scatolaStanzeByScatola.has(key)) {
                    serviziState.scatolaStanzeByScatola.set(key, row.stanze_presenti || '-');
                }
            });
        }

        renderSelects();
        renderServiziTables();
        if (!silent) {
            showServiziMsg(`Dati aggiornati: ${serviziState.stanze.length} stanze, ${serviziState.mobili.length} mobili, ${serviziState.scatole.length} scatole.`, 'ok');
        }
    };

    if (formStanze) formStanze.onsubmit = async (e) => {
        e.preventDefault();
        const id = document.getElementById('stanzaId').value;
        const nome = document.getElementById('stanzaNome').value.trim();
        const note = normalizeText(document.getElementById('stanzaNote').value);

        if (!nome) {
            showServiziMsg('Inserisci il nome della stanza.', 'err');
            return;
        }

        let result;
        if (id) {
            result = await supabase.from('stanze').update({ nome, note }).eq('id', id);
        } else {
            result = await supabase.from('stanze').insert({ nome, note });
        }

        if (result.error) {
            showServiziMsg(`Salvataggio stanza fallito: ${result.error.message}`, 'err');
            return;
        }

        resetStanzaForm();
        closeAllModals();
        await loadServiziData();
    };

    if (formMobili) formMobili.onsubmit = async (e) => {
        e.preventDefault();
        const id = document.getElementById('mobileId').value;
        const nome = document.getElementById('mobileNome').value.trim();
        const idstanza = document.getElementById('mobileStanza').value;
        const note = normalizeText(document.getElementById('mobileNote').value);

        if (!nome || !idstanza) {
            showServiziMsg('Compila nome mobile e stanza.', 'err');
            return;
        }

        const mobileCorrente = serviziState.mobili.find(m => String(m.id) === String(id));
        // La colonna foto e NOT NULL nello schema attuale.
        const payload = { nome, idstanza, note, foto: mobileCorrente?.foto || '' };
        let result;

        if (id) {
            result = await supabase.from('mobili').update(payload).eq('id', id);
        } else {
            result = await supabase.from('mobili').insert(payload);
        }

        if (result.error) {
            showServiziMsg(`Salvataggio mobile fallito: ${result.error.message}`, 'err');
            return;
        }

        resetMobileForm();
        closeAllModals();
        await loadServiziData();
    };

    if (formScatole) formScatole.onsubmit = async (e) => {
        e.preventDefault();
        const id = document.getElementById('scatolaId').value;
        const idscatola = id ? Number(id) : null;
        const nome = idscatola ? normalizeText(document.getElementById('scatolaNome').value) : null;
        const note = normalizeText(document.getElementById('scatolaNote').value);

        const scatolaCorrente = id
            ? serviziState.scatole.find(s => String(s.id) === String(id))
            : null;
        let pathfoto = scatolaCorrente?.pathfoto || null;
        const oldPathfoto = scatolaCorrente?.pathfoto || null;

        if (serviziState.scatolaFotoRemoved) {
            pathfoto = null;
        }

        if (oldPathfoto && serviziState.scatolaFotoRemoved && serviziState.scatolaFotoFile) {
            showServiziMsg('Per sostituire la foto: 1) rimuovi e salva, 2) riapri e carica la nuova foto.', 'err');
            return;
        }

        if (serviziState.scatolaFotoFile) {
            const compressed = await compressImage(serviziState.scatolaFotoFile);
            const filename = `scatole/${id || 'new'}-${Date.now()}.jpg`;
            const uploadRes = await uploadWithBucketFallback(filename, compressed, { upsert: true, contentType: 'image/jpeg' });

            if (uploadRes.error) {
                const bucketInfo = uploadRes.attemptedBuckets?.length
                    ? ` (bucket provati: ${uploadRes.attemptedBuckets.join(', ')})`
                    : '';
                showServiziMsg(`Upload foto scatola fallito: ${uploadRes.error.message}${bucketInfo}`, 'err');
                return;
            }

            pathfoto = filename;
        }

        const payloadBase = { nome, note, pathfoto };
        let idScatolaTarget = idscatola;

        let createdScatolaNumber = null;

        if (idscatola) {
            const updateScatolaRes = await supabase.from('scatole').update(payloadBase).eq('id', idscatola);
            if (updateScatolaRes.error) {
                showServiziMsg(`Aggiornamento scatola fallito: ${updateScatolaRes.error.message}`, 'err');
                return;
            }

            if (serviziState.scatolaFotoRemoved && oldPathfoto) {
                const removeRes = await removeWithBucketFallback(oldPathfoto);
                if (removeRes.error) {
                    showServiziMsg(`Foto rimossa dal record ma non dal bucket: ${removeRes.error.message}`, 'err');
                    return;
                }
            }
        } else {
            const insertScatolaRes = await supabase
                .from('scatole')
                .insert(payloadBase)
                .select('id')
                .single();

            if (insertScatolaRes.error || !insertScatolaRes.data?.id) {
                showServiziMsg(`Inserimento scatola fallito: ${insertScatolaRes.error?.message || 'ID non restituito'}`, 'err');
                return;
            }

            idScatolaTarget = Number(insertScatolaRes.data.id);
            createdScatolaNumber = idScatolaTarget;

            const setAutoNameRes = await supabase
                .from('scatole')
                .update({ nome: String(idScatolaTarget) })
                .eq('id', idScatolaTarget);

            if (setAutoNameRes.error) {
                showServiziMsg(`Scatola creata ma nome automatico non impostato: ${setAutoNameRes.error.message}`, 'err');
                return;
            }
        }

        resetScatolaForm();
        closeAllModals();
        await loadServiziData({ silent: Boolean(createdScatolaNumber) });

        if (createdScatolaNumber) {
            showServiziMsg(`Scatola creata. Numero scatola assegnato: ${createdScatolaNumber}.`, 'ok');
        }
    };

    panel?.addEventListener('click', async (event) => {
        const actionElement = event.target.closest('[data-action]');
        if (!actionElement) return;

        const action = actionElement.dataset.action;
        const id = actionElement.dataset.id;

        if (action === 'select-stanza') {
            const stanza = serviziState.stanze.find((item) => String(item.id) === String(id));
            if (!stanza) return;

            const isSameSelected = String(serviziState.selectedStanzaId || '') === String(stanza.id);
            serviziState.selectedStanzaId = isSameSelected ? null : String(stanza.id);
            renderServiziTables();

            if (isSameSelected) {
                showServiziMsg(`Stanza ${stanza.nome} chiusa.`, 'ok');
                return;
            }

            const linkedMobiliCount = serviziState.mobili.filter((mobile) => String(mobile.idstanza) === String(stanza.id)).length;
            showServiziMsg(`Stanza selezionata: ${stanza.nome}. Mobili collegati: ${linkedMobiliCount}.`, 'ok');
            return;
        }

        if (action === 'edit-stanza') {
            setServiziView('stanze');
            openEditStanzaById(id);
            return;
        }

        if (action === 'edit-mobile') {
            setServiziView('stanze');
            const mobile = serviziState.mobili.find(m => String(m.id) === String(id));
            if (!mobile) return;
            document.getElementById('mobileId').value = mobile.id;
            document.getElementById('mobileNome').value = mobile.nome || '';
            document.getElementById('mobileStanza').value = String(mobile.idstanza || '');
            document.getElementById('mobileNote').value = mobile.note || '';
            openModal('mobili');
            showServiziMsg(`Modifica mobile #${mobile.id}`, 'ok');
            return;
        }

        if (action === 'new-mobile-for-stanza') {
            setServiziView('stanze');
            resetMobileForm();
            document.getElementById('mobileStanza').value = String(id || '');
            openModal('mobili');
            showServiziMsg(`Nuovo mobile per stanza #${id}.`, 'ok');
            return;
        }

        if (action === 'edit-scatola') {
            openScatolaEditor(id, { fotoMode: false });
            return;
        }

        if (action === 'view-scatola-foto') {
            openScatolaEditor(id, { fotoMode: true });
            return;
        }

        if (action === 'print-scatola-label') {
            const scatola = serviziState.scatole.find(s => String(s.id) === String(id));
            if (!scatola) {
                showServiziMsg('Scatola non trovata per la stampa etichetta.', 'err');
                return;
            }

            const includeFragile = await askFragileLabelChoice();
            if (includeFragile === null) {
                showServiziMsg('Stampa etichetta annullata.', 'ok');
                return;
            }
            printScatolaLabelA5(scatola, includeFragile);
            showServiziMsg(`Aperta etichetta A5 per scatola #${scatola.id}. Usa il bottone Stampa nella pannellata.`, 'ok');
            return;
        }

        if (action === 'delete-stanza') await deleteStanzaWithChecks(id);
        if (action === 'delete-mobile') await deleteMobileWithChecks(id);
        if (action === 'delete-scatola') await deleteScatolaWithChecks(id);
    });

    if (isScatoleMode) {
        panel?.addEventListener('dblclick', (event) => {
            const targetEl = event.target;
            if (!(targetEl instanceof Element)) return;
            if (targetEl.closest('button, a, input, select, textarea, label')) return;

            const row = targetEl.closest('#tbScatole tr[data-scatola-id]');
            if (!row) return;

            const scatolaId = row.getAttribute('data-scatola-id') || '';
            const scatola = serviziState.scatole.find((item) => String(item.id) === String(scatolaId));
            if (!scatola) return;

            setGestionePrefill({
                scatola: getScatolaDisplayName(scatola),
            });
            caricaPannello('gestione');
        });
    }

    if (btnExpandMobiliTree) {
        btnExpandMobiliTree.onclick = () => {
            panel?.querySelectorAll('details[data-tree-node]').forEach((node) => {
                node.open = true;
            });
        };
    }
    if (btnCollapseMobiliTree) {
        btnCollapseMobiliTree.onclick = () => {
            panel?.querySelectorAll('details[data-tree-node]').forEach((node) => {
                node.open = false;
            });
        };
    }
    if (openStanzaModal) {
        openStanzaModal.onclick = () => {
            setServiziView('stanze');
            resetStanzaForm();
            openModal('stanze');
        };
    }
    if (editSelectedStanza) {
        editSelectedStanza.onclick = () => {
            setServiziView('stanze');
            if (!serviziState.selectedStanzaId) {
                showServiziMsg('Seleziona prima una stanza dall\'albero.', 'err');
                return;
            }
            openEditStanzaById(serviziState.selectedStanzaId);
        };
    }
    if (deleteSelectedStanza) {
        deleteSelectedStanza.onclick = async () => {
            setServiziView('stanze');
            await deleteStanzaWithChecks(serviziState.selectedStanzaId);
        };
    }
    if (openMobileModal) {
        openMobileModal.onclick = () => {
            setServiziView('stanze');
            resetMobileForm();
            openModal('mobili');
        };
    }
    if (openScatolaModal) {
        openScatolaModal.onclick = () => {
            setServiziView('scatole');
            resetScatolaForm();
            setScatolaModalMode(false);
            openModal('scatole');
        };
    }
    if (btnViewStanze) btnViewStanze.onclick = () => setServiziView('stanze');
    if (btnViewScatole) btnViewScatole.onclick = () => setServiziView('scatole');
    if (resetStanza) {
        resetStanza.onclick = () => {
            resetStanzaForm();
            closeAllModals();
        };
    }
    if (resetMobile) {
        resetMobile.onclick = () => {
            resetMobileForm();
            closeAllModals();
        };
    }
    if (resetScatola) {
        resetScatola.onclick = () => {
            resetScatolaForm();
            setScatolaModalMode(false);
            closeAllModals();
        };
    }

    if (filterScatolaNome) {
        filterScatolaNome.onchange = () => {
            serviziState.filters.scatolaNome = filterScatolaNome.value || '';
            renderServiziTables();
        };
    }

    if (filterScatolaStanza) {
        filterScatolaStanza.onchange = () => {
            serviziState.filters.stanza = filterScatolaStanza.value || '';
            renderServiziTables();
        };
    }
    panel?.querySelectorAll('button[data-close-modal]').forEach((btn) => {
        btn.onclick = closeAllModals;
    });

    if (modalOverlay) {
        modalOverlay.onclick = (event) => {
            if (event.target === modalOverlay) closeAllModals();
        };
    }

    panel?.addEventListener('keydown', (event) => {
        if (event.key === 'Escape') closeAllModals();
    });

    if (btnScatolaFoto && scatolaFotoFile) {
        btnScatolaFoto.onclick = () => {
            const scatolaId = document.getElementById('scatolaId')?.value;
            const scatolaCorrente = scatolaId
                ? serviziState.scatole.find((s) => String(s.id) === String(scatolaId))
                : null;

            if (scatolaCorrente?.pathfoto) {
                showServiziMsg('Per modificare la foto devi prima cliccare "Rimuovi Foto" e salvare.', 'err');
                return;
            }

            scatolaFotoFile.click();
        };
    }

    if (scatolaFotoFile) {
        scatolaFotoFile.onchange = () => {
            const file = scatolaFotoFile.files?.[0];
            if (!file) return;

            const scatolaId = document.getElementById('scatolaId')?.value;
            const scatolaCorrente = scatolaId
                ? serviziState.scatole.find((s) => String(s.id) === String(scatolaId))
                : null;
            if (scatolaCorrente?.pathfoto) {
                scatolaFotoFile.value = '';
                showServiziMsg('Foto gia presente: per sostituirla devi prima rimuoverla e salvare.', 'err');
                return;
            }

            serviziState.scatolaFotoFile = file;
            serviziState.scatolaFotoRemoved = false;

            const info = document.getElementById('scatolaFotoInfo');
            const preview = document.getElementById('scatolaFotoPreview');
            if (info) info.textContent = `Nuova foto: ${file.name}`;
            if (preview) {
                preview.src = URL.createObjectURL(file);
                preview.style.display = 'block';
            }
        };
    }

    if (btnRimuoviScatolaFoto) {
        btnRimuoviScatolaFoto.onclick = () => {
            const scatolaId = document.getElementById('scatolaId')?.value;
            const scatolaCorrente = scatolaId
                ? serviziState.scatole.find((s) => String(s.id) === String(scatolaId))
                : null;

            if (!scatolaCorrente?.pathfoto && !serviziState.scatolaFotoFile) {
                showServiziMsg('Nessuna foto da rimuovere.', 'err');
                return;
            }

            serviziState.scatolaFotoFile = null;
            serviziState.scatolaFotoRemoved = Boolean(scatolaCorrente?.pathfoto);
            if (scatolaFotoFile) scatolaFotoFile.value = '';
            resetScatolaPhotoUi();
            showServiziMsg('Foto marcata per rimozione. Salva per confermare.', 'ok');
        };
    }

    try {
        setServiziView(isScatoleMode ? 'scatole' : 'stanze');
        setScatolaModalMode(false);
        await loadServiziData();
    } catch (err) {
        showServiziMsg(err.message, 'err');
    }
}

async function salvaOggetto(options = {}) {
    const inputFoto = document.getElementById('fotoOggetto');
    const selScatola = document.getElementById('selScatola');
    const nomeOggetto = document.getElementById('nomeOggetto');
    const form = document.getElementById('formOggetto');
    const preview = document.getElementById('preview');
    const noteOggetto = document.getElementById('noteOggetto');
    const selectedStanzaId = options.stanzaId ? Number(options.stanzaId) : null;
    const selectedMobileId = options.mobileId ? Number(options.mobileId) : null;
    const confirmedScatolaId = options.confirmedScatolaId ? Number(options.confirmedScatolaId) : null;

    if (!inputFoto?.files?.[0]) {
        window.alert('Seleziona prima una foto.');
        return false;
    }

    if (!confirmedScatolaId || !selScatola?.value || selScatola.value === 'new') {
        window.alert('Seleziona una scatola valida.');
        return false;
    }

    const compressed = await compressImage(inputFoto.files[0]);
    const filename = `oggetti/${Date.now()}.jpg`;

    const uploadRes = await uploadWithBucketFallback(filename, compressed, { upsert: true, contentType: 'image/jpeg' });

    if (uploadRes.error) {
        const bucketInfo = uploadRes.attemptedBuckets?.length
            ? ` (bucket provati: ${uploadRes.attemptedBuckets.join(', ')})`
            : '';
        window.alert(`Upload foto fallito: ${uploadRes.error.message}${bucketInfo}`);
        return false;
    }

    const basePayload = {
        nome: nomeOggetto?.value?.trim() || null,
        idscatola: confirmedScatolaId,
        pathfoto: filename,
        thumbnail: filename,
        note: normalizeText(options.note ?? noteOggetto?.value),
    };

    const payloadWithLocation = {
        ...basePayload,
        idstanza: selectedStanzaId,
        idmobile: selectedMobileId,
    };

    let insertRes = await supabase
        .from('oggetti')
        .insert(payloadWithLocation)
        .select('id,pathfoto')
        .single();

    // Fallback per schema legacy che non ha ancora idstanza/idmobile su oggetti.
    if (insertRes.error && /idstanza|idmobile|schema\s+cache|column/i.test(insertRes.error.message || '')) {
        insertRes = await supabase
            .from('oggetti')
            .insert(basePayload)
            .select('id,pathfoto')
            .single();
    }

    const { data, error } = insertRes;

    if (error || !data?.id) {
        window.alert(`Salvataggio oggetto fallito: ${error?.message || 'ID non restituito'}`);
        return false;
    }

    if (form) form.reset();
    if (preview) {
        preview.src = '';
        preview.style.display = 'none';
    }

    window.alert('Oggetto archiviato correttamente.');
    return data;
}

// Funzione utility compressione
async function compressImage(file) {
    return new Promise(resolve => {
        const img = new Image();
        img.src = URL.createObjectURL(file);
        img.onload = () => {
            const canvas = document.createElement('canvas');
            const MAX = 800;
            let w = img.width, h = img.height;
            if (w > MAX) { h *= MAX / w; w = MAX; }
            canvas.width = w; canvas.height = h;
            canvas.getContext('2d').drawImage(img, 0, 0, w, h);
            canvas.toBlob(resolve, 'image/jpeg', 0.7);
        };
    });
}

function setGestionePrefill(prefill) {
    if (!prefill || typeof prefill !== 'object') return;
    try {
        window.sessionStorage.setItem(GESTIONE_PREFILL_KEY, JSON.stringify(prefill));
    } catch {
        // Ignore storage persistence errors.
    }
}

function consumeGestionePrefill() {
    try {
        const raw = window.sessionStorage.getItem(GESTIONE_PREFILL_KEY);
        if (!raw) return null;
        window.sessionStorage.removeItem(GESTIONE_PREFILL_KEY);
        const parsed = JSON.parse(raw);
        return parsed && typeof parsed === 'object' ? parsed : null;
    } catch {
        return null;
    }
}

function formatBackupTimestamp(date = new Date()) {
    const pad = (n) => String(n).padStart(2, '0');
    return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}_${pad(date.getHours())}-${pad(date.getMinutes())}-${pad(date.getSeconds())}`;
}

function arrayBufferToBase64(buffer) {
    const bytes = new Uint8Array(buffer);
    let binary = '';
    const chunkSize = 0x8000;
    for (let i = 0; i < bytes.length; i += chunkSize) {
        const chunk = bytes.subarray(i, i + chunkSize);
        binary += String.fromCharCode(...chunk);
    }
    return btoa(binary);
}

function base64ToUint8Array(base64) {
    const binary = atob(base64);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i += 1) {
        bytes[i] = binary.charCodeAt(i);
    }
    return bytes;
}

function getBackupFilePaths(payload) {
    const paths = new Set();
    for (const row of payload.oggetti || []) {
        if (row.pathfoto) paths.add(row.pathfoto);
        if (row.thumbnail) paths.add(row.thumbnail);
    }
    for (const row of payload.scatole || []) {
        if (row.pathfoto) paths.add(row.pathfoto);
    }
    for (const row of payload.mobili || []) {
        if (row.foto) paths.add(row.foto);
    }
    return Array.from(paths);
}

function estimateBase64Bytes(base64) {
    if (!base64) return 0;
    const len = base64.length;
    const padding = base64.endsWith('==') ? 2 : (base64.endsWith('=') ? 1 : 0);
    return Math.max(0, Math.floor((len * 3) / 4) - padding);
}

function toHex(bytes) {
    return Array.from(bytes).map((b) => b.toString(16).padStart(2, '0')).join('');
}

function simpleChecksumHex(text) {
    let hash = 2166136261;
    for (let i = 0; i < text.length; i += 1) {
        hash ^= text.charCodeAt(i);
        hash = Math.imul(hash, 16777619);
    }
    return `fnv1a32-${(hash >>> 0).toString(16).padStart(8, '0')}`;
}

async function computeChecksumHex(text) {
    if (window.crypto?.subtle && window.TextEncoder) {
        const encoded = new TextEncoder().encode(text);
        const digest = await window.crypto.subtle.digest('SHA-256', encoded);
        return `sha256-${toHex(new Uint8Array(digest))}`;
    }
    return simpleChecksumHex(text);
}

function buildBackupSummary(corePayload) {
    const tables = corePayload.tables || {};
    const files = normalizeBackupRows(corePayload.files);
    const totalFileBytes = files.reduce((sum, file) => sum + estimateBase64Bytes(file.base64 || ''), 0);

    return {
        stanzeCount: normalizeBackupRows(tables.stanze).length,
        mobiliCount: normalizeBackupRows(tables.mobili).length,
        scatoleCount: normalizeBackupRows(tables.scatole).length,
        oggettiCount: normalizeBackupRows(tables.oggetti).length,
        filesCount: files.length,
        totalFileBytes,
    };
}

function getBackupIntegritySource(corePayload) {
    return JSON.stringify({
        app: corePayload.app,
        schemaVersion: corePayload.schemaVersion,
        createdAt: corePayload.createdAt,
        sourceBucket: corePayload.sourceBucket,
        tables: corePayload.tables,
        files: corePayload.files,
    });
}

async function attachBackupIntegrity(corePayload) {
    const source = getBackupIntegritySource(corePayload);
    const checksum = await computeChecksumHex(source);
    return {
        ...corePayload,
        integrity: {
            checksum,
            summary: buildBackupSummary(corePayload),
        },
    };
}

async function validateBackupIntegrity(backupPayload) {
    const corePayload = {
        app: backupPayload?.app,
        schemaVersion: backupPayload?.schemaVersion,
        createdAt: backupPayload?.createdAt,
        sourceBucket: backupPayload?.sourceBucket,
        tables: backupPayload?.tables,
        files: backupPayload?.files,
    };
    const summary = buildBackupSummary(corePayload);

    if (!backupPayload?.integrity?.checksum) {
        return {
            ok: false,
            reason: 'missing-checksum',
            summary,
        };
    }

    const expected = String(backupPayload.integrity.checksum);
    const actual = await computeChecksumHex(getBackupIntegritySource(corePayload));
    return {
        ok: actual === expected,
        reason: actual === expected ? '' : 'checksum-mismatch',
        summary,
        expected,
        actual,
    };
}

async function downloadWithBucketFallback(path, extra = []) {
    const candidateSets = [getStorageBucketCandidates(extra)];
    const attemptedBuckets = [];
    let lastError = null;

    for (const candidates of candidateSets) {
        for (const bucketName of candidates) {
            attemptedBuckets.push(bucketName);
            const result = await supabase.storage.from(bucketName).download(path);
            if (!result.error && result.data) {
                setCurrentStorageBucket(bucketName);
                return { data: result.data, error: null, bucket: bucketName, attemptedBuckets };
            }

            lastError = result.error;
            if (!isBucketNotFoundError(result.error)) {
                if (isStorageObjectNotFoundError(result.error)) {
                    continue;
                }
                return { data: null, error: result.error, bucket: bucketName, attemptedBuckets };
            }
        }

        if (candidateSets.length === 1) {
            const bucketNamesFromApi = await fetchBucketNames();
            const additional = getStorageBucketCandidates(bucketNamesFromApi)
                .filter((name) => !candidates.includes(name));
            if (additional.length > 0) candidateSets.push(additional);
        }
    }

    return { data: null, error: lastError || new Error('File storage non trovato'), bucket: null, attemptedBuckets };
}

async function buildLocalBackup() {
    const [stanzeRes, mobiliRes, scatoleRes, oggettiRes] = await Promise.all([
        supabase.from('stanze').select('id,nome,note').order('id', { ascending: true }),
        supabase.from('mobili').select('id,idstanza,nome,note,foto').order('id', { ascending: true }),
        supabase.from('scatole').select('id,nome,pathfoto,note').order('id', { ascending: true }),
        supabase.from('oggetti').select('id,nome,idscatola,idstanza,idmobile,pathfoto,thumbnail,note').order('id', { ascending: true }),
    ]);

    const firstError = stanzeRes.error || mobiliRes.error || scatoleRes.error || oggettiRes.error;
    if (firstError) throw new Error(firstError.message || 'Errore caricamento dati da salvare');

    const tables = {
        stanze: stanzeRes.data || [],
        mobili: mobiliRes.data || [],
        scatole: scatoleRes.data || [],
        oggetti: oggettiRes.data || [],
    };

    const files = [];
    for (const path of getBackupFilePaths(tables)) {
        const downloadRes = await downloadWithBucketFallback(path);
        if (!downloadRes.data) continue;

        const blob = downloadRes.data;
        const buffer = await blob.arrayBuffer();
        files.push({
            path,
            bucket: downloadRes.bucket,
            mimeType: blob.type || 'application/octet-stream',
            base64: arrayBufferToBase64(buffer),
        });
    }

    const corePayload = {
        app: 'Trasloco Smart',
        schemaVersion: 1,
        createdAt: new Date().toISOString(),
        sourceBucket: storageState.bucket,
        tables,
        files,
    };

    return attachBackupIntegrity(corePayload);
}

function downloadBackupFile(payload) {
    const filename = `trasloco-backup-${formatBackupTimestamp()}.json`;
    const blob = new Blob([JSON.stringify(payload, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
}

function normalizeBackupRows(value) {
    return Array.isArray(value) ? value : [];
}

async function purgeAllDataForRestore() {
    const tables = ['oggetti', 'mobili', 'scatole', 'stanze'];
    for (const table of tables) {
        const { error } = await supabase.from(table).delete().not('id', 'is', null);
        if (error) throw new Error(`Pulizia tabella ${table} fallita: ${error.message}`);
    }
}

async function restoreStorageFiles(files) {
    for (const file of files) {
        if (!file?.path || !file?.base64) continue;
        const bytes = base64ToUint8Array(file.base64);
        const uploadRes = await uploadWithBucketFallback(file.path, new Blob([bytes], { type: file.mimeType || 'application/octet-stream' }), {
            upsert: true,
            contentType: file.mimeType || 'application/octet-stream',
        });

        if (uploadRes.error) {
            throw new Error(`Ripristino file ${file.path} fallito: ${uploadRes.error.message}`);
        }
    }
}

async function restoreTablesWithMapping(tables) {
    const stanze = normalizeBackupRows(tables.stanze).sort((a, b) => Number(a.id || 0) - Number(b.id || 0));
    const mobili = normalizeBackupRows(tables.mobili).sort((a, b) => Number(a.id || 0) - Number(b.id || 0));
    const scatole = normalizeBackupRows(tables.scatole).sort((a, b) => Number(a.id || 0) - Number(b.id || 0));
    const oggetti = normalizeBackupRows(tables.oggetti).sort((a, b) => Number(a.id || 0) - Number(b.id || 0));

    const stanzaMap = new Map();
    const mobileMap = new Map();
    const scatolaMap = new Map();

    for (const row of stanze) {
        const payload = {
            nome: row.nome,
            note: normalizeText(row.note),
        };
        const { data, error } = await supabase.from('stanze').insert(payload).select('id').single();
        if (error || !data?.id) throw new Error(`Ripristino stanza fallito: ${error?.message || 'ID mancante'}`);
        stanzaMap.set(String(row.id), Number(data.id));
    }

    for (const row of mobili) {
        const mappedStanzaId = stanzaMap.get(String(row.idstanza));
        if (!mappedStanzaId) continue;
        const payload = {
            idstanza: mappedStanzaId,
            nome: row.nome,
            note: normalizeText(row.note),
            foto: row.foto || '',
        };
        const { data, error } = await supabase.from('mobili').insert(payload).select('id').single();
        if (error || !data?.id) throw new Error(`Ripristino mobile fallito: ${error?.message || 'ID mancante'}`);
        mobileMap.set(String(row.id), Number(data.id));
    }

    for (const row of scatole) {
        const payload = {
            nome: normalizeText(row.nome),
            pathfoto: normalizeText(row.pathfoto),
            note: normalizeText(row.note),
        };
        const { data, error } = await supabase.from('scatole').insert(payload).select('id').single();
        if (error || !data?.id) throw new Error(`Ripristino scatola fallito: ${error?.message || 'ID mancante'}`);
        scatolaMap.set(String(row.id), Number(data.id));
    }

    for (const row of oggetti) {
        const mappedScatolaId = scatolaMap.get(String(row.idscatola));
        const mappedStanzaId = stanzaMap.get(String(row.idstanza));
        const mappedMobileId = row.idmobile ? (mobileMap.get(String(row.idmobile)) || null) : null;
        if (!mappedScatolaId || !mappedStanzaId) continue;

        const payload = {
            nome: normalizeText(row.nome),
            idscatola: mappedScatolaId,
            idstanza: mappedStanzaId,
            idmobile: mappedMobileId,
            pathfoto: row.pathfoto,
            thumbnail: normalizeText(row.thumbnail),
            note: normalizeText(row.note),
        };

        const { error } = await supabase.from('oggetti').insert(payload);
        if (error) throw new Error(`Ripristino oggetto fallito: ${error.message}`);
    }
}

async function restoreFromBackupPayload(backupPayload) {
    const tables = backupPayload?.tables;
    if (!tables || typeof tables !== 'object') {
        throw new Error('Backup non valido: sezione tabelle mancante.');
    }

    await restoreStorageFiles(normalizeBackupRows(backupPayload.files));
    await purgeAllDataForRestore();
    await restoreTablesWithMapping(tables);
}

function readJsonFile(file) {
    return new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onload = () => {
            try {
                const parsed = JSON.parse(String(reader.result || '{}'));
                resolve(parsed);
            } catch (error) {
                reject(error);
            }
        };
        reader.onerror = () => reject(new Error('Impossibile leggere il file di backup.'));
        reader.readAsText(file);
    });
}

function resolveDriveClientId() {
    if (driveState.clientId) return driveState.clientId;

    try {
        const saved = window.localStorage.getItem(DRIVE_CLIENT_ID_STORAGE_KEY);
        if (saved) {
            driveState.clientId = saved;
            return driveState.clientId;
        }
    } catch {
        // Ignore storage errors.
    }

    const fromWindow = typeof window.TRASLOCO_GOOGLE_CLIENT_ID === 'string'
        ? window.TRASLOCO_GOOGLE_CLIENT_ID.trim()
        : '';
    if (fromWindow) {
        driveState.clientId = fromWindow;
        try {
            window.localStorage.setItem(DRIVE_CLIENT_ID_STORAGE_KEY, driveState.clientId);
        } catch {
            // Ignore storage errors.
        }
        return driveState.clientId;
    }

    const entered = window.prompt('Inserisci Google OAuth Client ID (Web) per backup su Drive:');
    const value = String(entered || '').trim();
    if (!value) {
        throw new Error('Client ID Google mancante.');
    }
    driveState.clientId = value;
    try {
        window.localStorage.setItem(DRIVE_CLIENT_ID_STORAGE_KEY, driveState.clientId);
    } catch {
        // Ignore storage errors.
    }
    return driveState.clientId;
}

function isDriveTokenValid() {
    return Boolean(driveState.accessToken) && Date.now() < driveState.tokenExpiresAt - 10000;
}

async function ensureDriveAccessToken() {
    if (isDriveTokenValid()) return driveState.accessToken;

    if (!window.google?.accounts?.oauth2) {
        throw new Error('Google Identity Services non disponibile. Ricarica la pagina.');
    }

    const clientId = resolveDriveClientId();
    const tokenResponse = await new Promise((resolve, reject) => {
        const tokenClient = window.google.accounts.oauth2.initTokenClient({
            client_id: clientId,
            scope: DRIVE_SCOPE,
            callback: (response) => {
                if (response?.error) {
                    reject(new Error(response.error_description || response.error || 'Autorizzazione Drive fallita.'));
                    return;
                }
                resolve(response);
            },
        });

        tokenClient.requestAccessToken({ prompt: 'consent' });
    });

    driveState.accessToken = tokenResponse.access_token || '';
    driveState.tokenExpiresAt = Date.now() + (Number(tokenResponse.expires_in || 3600) * 1000);
    if (!driveState.accessToken) {
        throw new Error('Token Drive non ricevuto.');
    }
    return driveState.accessToken;
}

async function driveFetchJson(url, options = {}) {
    const token = await ensureDriveAccessToken();
    const response = await fetch(url, {
        ...options,
        headers: {
            Authorization: `Bearer ${token}`,
            ...(options.headers || {}),
        },
    });

    if (!response.ok) {
        const text = await response.text();
        throw new Error(`Drive API ${response.status}: ${text || response.statusText}`);
    }

    if (response.status === 204) return null;
    return response.json();
}

async function uploadBackupToDrive(backupPayload) {
    const filename = `${DRIVE_BACKUP_PREFIX}${formatBackupTimestamp()}.json`;
    const metadata = {
        name: filename,
        parents: [DRIVE_FOLDER_ID],
        mimeType: 'application/json',
    };

    const boundary = `trasloco_${Date.now()}`;
    const body = [
        `--${boundary}`,
        'Content-Type: application/json; charset=UTF-8',
        '',
        JSON.stringify(metadata),
        `--${boundary}`,
        'Content-Type: application/json; charset=UTF-8',
        '',
        JSON.stringify(backupPayload),
        `--${boundary}--`,
    ].join('\r\n');

    const created = await driveFetchJson('https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id,name,createdTime', {
        method: 'POST',
        headers: {
            'Content-Type': `multipart/related; boundary=${boundary}`,
        },
        body,
    });

    return created;
}

async function listDriveBackups() {
    const q = encodeURIComponent(`'${DRIVE_FOLDER_ID}' in parents and trashed=false and name contains '${DRIVE_BACKUP_PREFIX}' and mimeType='application/json'`);
    const fields = encodeURIComponent('files(id,name,createdTime,size),nextPageToken');
    const result = await driveFetchJson(`https://www.googleapis.com/drive/v3/files?q=${q}&orderBy=createdTime desc&fields=${fields}&pageSize=50`);
    return Array.isArray(result?.files) ? result.files : [];
}

async function enforceDriveRetention() {
    const backups = await listDriveBackups();
    const obsolete = backups.slice(DRIVE_RETENTION_COUNT);

    for (const file of obsolete) {
        await driveFetchJson(`https://www.googleapis.com/drive/v3/files/${encodeURIComponent(file.id)}`, {
            method: 'DELETE',
        });
    }

    return {
        kept: backups.slice(0, DRIVE_RETENTION_COUNT),
        deleted: obsolete.length,
    };
}

async function downloadDriveBackupById(fileId) {
    const token = await ensureDriveAccessToken();
    const response = await fetch(`https://www.googleapis.com/drive/v3/files/${encodeURIComponent(fileId)}?alt=media`, {
        headers: {
            Authorization: `Bearer ${token}`,
        },
    });

    if (!response.ok) {
        const text = await response.text();
        throw new Error(`Download backup da Drive fallito (${response.status}): ${text || response.statusText}`);
    }

    return response.json();
}

async function chooseDriveBackupForRestore() {
    const backups = await listDriveBackups();
    if (backups.length === 0) {
        throw new Error('Nessun backup trovato nella cartella Drive configurata.');
    }

    const lines = backups.map((file, index) => {
        const created = file.createdTime ? new Date(file.createdTime).toLocaleString('it-IT') : 'data sconosciuta';
        return `${index + 1}) ${file.name} - ${created}`;
    });

    const choice = window.prompt(`Seleziona backup da ripristinare (numero):\n${lines.join('\n')}`);
    const idx = Number(choice);
    if (!Number.isInteger(idx) || idx < 1 || idx > backups.length) {
        throw new Error('Selezione backup non valida.');
    }

    return backups[idx - 1];
}

function initBackupControl() {
    const btn = document.getElementById('btnBackupControl');
    if (!btn) return;

    btn.onclick = async () => {
        const action = window.prompt('Backup dati su Google Drive:\n- digita S per SALVARE backup\n- digita R per RIPRISTINARE backup');
        const normalized = String(action || '').trim().toUpperCase();
        if (!normalized) return;

        if (normalized === 'S') {
            try {
                const backup = await buildLocalBackup();
                const created = await uploadBackupToDrive(backup);
                const retention = await enforceDriveRetention();
                const summary = backup.integrity?.summary || buildBackupSummary(backup);
                const checksumShort = String(backup.integrity?.checksum || '').slice(0, 18);
                window.alert(`Backup su Drive completato.\nFile: ${created?.name || '-'}\nStanze: ${summary.stanzeCount}\nMobili: ${summary.mobiliCount}\nScatole: ${summary.scatoleCount}\nOggetti: ${summary.oggettiCount}\nFile multimediali: ${summary.filesCount}\nRetention: ultime ${DRIVE_RETENTION_COUNT} copie (eliminate ${retention.deleted})\nChecksum: ${checksumShort}...`);
            } catch (error) {
                window.alert(`Backup fallito: ${error.message || String(error)}`);
            }
            return;
        }

        if (normalized === 'R') {
            try {
                const selected = await chooseDriveBackupForRestore();
                const confirmMsg = `Ripristinare il backup ${selected.name}?\nI dati attuali su Supabase verranno sovrascritti.`;
                if (!window.confirm(confirmMsg)) return;

                const payload = await downloadDriveBackupById(selected.id);
                const validation = await validateBackupIntegrity(payload);
                if (!validation.ok) {
                    if (validation.reason === 'missing-checksum') {
                        const proceedLegacy = window.confirm('Backup senza checksum: integrita non verificabile. Continuare comunque?');
                        if (!proceedLegacy) return;
                    } else {
                        throw new Error('Integrita backup non valida (checksum diverso). Ripristino bloccato.');
                    }
                }

                await restoreFromBackupPayload(payload);
                window.alert('Ripristino da Drive completato con successo. Ricarico i pannelli.');
                caricaPannello('inserimento');
            } catch (error) {
                window.alert(`Ripristino fallito: ${error.message || String(error)}`);
            }
            return;
        }

        window.alert('Azione non riconosciuta. Usa S o R.');
    };
}
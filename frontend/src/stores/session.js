export const state = {session: JSON.parse(sessionStorage.getItem('demo-session') || 'null'), spec:null,
                      cases:[], runs:[], evidence:[], audit:[], draft:null,
                      ingestionBatches:[], ingestionReplays:[], ingestionLedger:null, segmentReceipt:null};

export function setSession(session) {
  state.session = session;
  state.spec = null;
  state.draft = null;
  if (session) sessionStorage.setItem('demo-session', JSON.stringify(session));
  else sessionStorage.removeItem('demo-session');
}

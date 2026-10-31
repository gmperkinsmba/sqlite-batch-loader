let requestToken = "";
const $ = (id) => document.getElementById(id);
const esc = (s) => String(s ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
async function api(path, body) {
  const options = body === undefined ? {} : {method:"POST",headers:{"Content-Type":"application/json","X-BD3-Token":requestToken},body:JSON.stringify(body)};
  const r = await fetch(path, options); const data = await r.json();
  if (!r.ok) throw new Error(data.error || `Request failed (${r.status})`); return data;
}
function count(n){return String(n ?? 0).padStart(2,"0")}
async function refresh(){
  const state = await api("/api/status");
  $("batchMetric").textContent=count(state.batches.length); $("recordsMetric").textContent=count(state.record_count);
  $("quarantineMetric").textContent=count(state.quarantines.length);
  const pending=state.queue.filter(q=>q.status==="pending").length; $("lagMetric").textContent=count(pending);
  $("queueCount").textContent=`${pending} batch${pending===1?"":"es"} pending`;
  $("recordCount").textContent=count(state.record_count);
  $("workerCountA").textContent=state.worker_shards.includes("worker-a.sqlite")?"READY":"0";
  $("workerCountB").textContent=state.worker_shards.includes("worker-b.sqlite")?"READY":"0";
  $("sinkStatus").textContent=state.batches.length?"CONNECTED":"WAITING";
  $("duckdbAvailability").textContent=state.duckdb_available?"Ready · local adapter installed":"Missing · install DuckDB package";
  $("runtimePath").textContent=state.runtime_path || "Private local folder";
  const receipts = new Map(state.queue.map(q=>[q.batch_id,q.status]));
  $("receiptRows").innerHTML = state.batches.length ? state.batches.map(b=>`<tr><td><div class="batch-id">${esc(b.batch_id)}</div><div class="sub-cell">${esc(b.committed_at)}</div></td><td>${esc(b.source_id)}<div class="sub-cell">revision ${esc(b.source_revision)}</div></td><td>${count(b.accepted)}<div class="sub-cell">of ${count(b.record_count)} rows</div></td><td>${count(b.duplicate_records)} replayed · ${count(b.tombstones)} removed</td><td><span class="status">COMMITTED</span></td><td><span class="status ${receipts.get(b.batch_id)==="pending"?"pending":""}">${receipts.get(b.batch_id)==="pending"?"PENDING":"VERIFIED"}</span></td></tr>`).join("") : '<tr class="empty"><td colspan="6">No batches yet. Run the sample intake to see a verified path.</td></tr>';
}
async function run(path, label){
  const button=$(path.endsWith("run")?"runDemo":"recoveryDemo"); button.disabled=true; $("feedback").textContent=`${label} in progress…`;
  try { const result=await api(path,{}); await refresh(); $("feedback").textContent=path.endsWith("run")?`Committed ${result.batches.length} batches across both shards and verified DuckDB receipts.`:`Recovered ${result.receipt.batch_id} from a restart after stage commit.`; }
  catch(e){$("feedback").textContent=e.message;$("feedback").classList.add("error");}
  finally{button.disabled=false;}
}
document.addEventListener("DOMContentLoaded", async()=>{
  try {requestToken=(await api("/api/session")).token; await refresh();}
  catch(e){$("feedback").textContent=`Local API unavailable: ${e.message}`;$("feedback").classList.add("error");}
  $("runDemo").addEventListener("click",()=>run("/api/demo/run","Sample intake"));
  $("recoveryDemo").addEventListener("click",()=>run("/api/demo/recovery","Recovery check"));
  $("refresh").addEventListener("click",()=>refresh().catch(e=>$("feedback").textContent=e.message));
  const dlg=$("settingsDialog"); $("settingsOpen").addEventListener("click",()=>dlg.showModal());
  $("saveSettings").addEventListener("click",async(e)=>{e.preventDefault();try{await api("/api/settings",{destination:$('destination').value==="duckdb"?"destination.duckdb":null});$("settingsFeedback").textContent="Settings saved for this local run.";}catch(err){$("settingsFeedback").textContent=err.message;}});
  dlg.addEventListener("click",e=>{if(e.target===dlg)dlg.close();});
});

const contentDiv = document.getElementById('content');
const venueTabsDiv = document.getElementById('venue-tabs');
const meetingContentDiv = document.getElementById('meeting-content');
const infoModal = document.getElementById('info-modal');
let allRacesData = {};

function openModal() { infoModal.style.display = 'block'; }
function closeModal() { infoModal.style.display = 'none'; }
window.onclick = function(event) {
    if (event.target == infoModal) closeModal();
}

const formatTime = (isoString) => {
     if (!isoString) return 'N/A';
     return new Date(isoString).toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' });
};

function processRaceData(race) {
    if (!race || !race.runs) return [];
    const activeRunners = race.runs.filter(r => !r.scratched && !r.isManuallyScratched && r.boxNumber);
    let totalAvgTime = 0, countAvgTime = 0, totalAvgSplit = 0, countAvgSplit = 0;
    activeRunners.forEach(runner => {
        if (runner.avgTimeLast5TrackDist) { totalAvgTime += runner.avgTimeLast5TrackDist; countAvgTime++; }
        if (runner.avgSplitLast5TrackDist) { totalAvgSplit += runner.avgSplitLast5TrackDist; countAvgSplit++; }
    });
    const fieldAvgTime = countAvgTime > 0 ? totalAvgTime / countAvgTime : null;
    const fieldAvgSplit = countAvgSplit > 0 ? totalAvgSplit / countAvgSplit : null;
    
    // Standard Rankings Logic
    const statsToRank = [
        { key: 'winRateAtTrack', higherIsBetter: true }, { key: 'placeRateAtTrack', higherIsBetter: true },
        { key: 'winRateAtDistance', higherIsBetter: true }, { key: 'placeRateAtDistance', higherIsBetter: true },
        { key: 'leadAtFirstBendRate', higherIsBetter: true }, { key: 'winRate', higherIsBetter: true },
        { key: 'placeRate', higherIsBetter: true }, { key: 'trainerStrikeRate', higherIsBetter: true },
        { key: 'boxWinPercentage', higherIsBetter: true }, { key: 'avgTimeLast5TrackDist', higherIsBetter: false },
        { key: 'avgSplitLast5TrackDist', higherIsBetter: false },
    ];
    
    race.runs.forEach(r => { 
        r.rankings = {}; 
        r.summary = []; 
    });

    statsToRank.forEach(stat => {
        const sortedRunners = activeRunners
            .filter(r => r[stat.key] !== null && r[stat.key] !== undefined && r[stat.key] > 0)
            .sort((a, b) => stat.higherIsBetter ? b[stat.key] - a[stat.key] : a[stat.key] - b[stat.key]);
        sortedRunners.slice(0, 3).forEach((runner, index) => { runner.rankings[stat.key] = index + 1; });
    });

    // Process per-runner calculations and Generate Summary
    race.runs.forEach(runner => {
        if (!runner.scratched && !runner.isManuallyScratched && runner.boxNumber) {
            runner.avgTdVsField = (runner.avgTimeLast5TrackDist && fieldAvgTime) ? runner.avgTimeLast5TrackDist - fieldAvgTime : undefined;
            runner.avgSplitVsField = (runner.avgSplitLast5TrackDist && fieldAvgSplit) ? runner.avgSplitLast5TrackDist - fieldAvgSplit : undefined;
                    
            const tags = [];
            const trkWin = (runner.winRateAtTrack || 0) * 100;
            const distWin = (runner.winRateAtDistance || 0) * 100;
            const overallWin = (runner.winRate || 0) * 100;
            const trkPlace = (runner.placeRateAtTrack || 0) * 100;
            const leadPct = (runner.leadAtFirstBendRate || 0) * 100;
            const vsField = runner.avgTdVsField || 99;
            const vsSplit = runner.avgSplitVsField || 99;
            const trainerSR = (runner.trainerStrikeRate || 0) * 100;

            // 1. Class Drop
            if (runner.isDownGrade) tags.push({ text: 'CLS DROP', class: 'tag-cls-drop', priority: 12 });

            // 2. Win Stats
            if (trkWin >= 30 && (runner.startsAtTrack || 0) >= 3) tags.push({ text: 'WIN TRK', class: 'tag-win-trk', priority: 10 });
            else if (distWin >= 30 && (runner.startsAtDistance || 0) >= 3) tags.push({ text: 'WIN DIST', class: 'tag-win-dist', priority: 9 });
            else if (overallWin >= 25) tags.push({ text: 'WIN', class: 'tag-win-strong', priority: 8 });

            // 3. Trainer
            if (trainerSR > 25) tags.push({ text: 'TRAINER', class: 'tag-trainer', priority: 11 });

            // 4. Form / Speed
            if (vsField <= -0.15) tags.push({ text: 'FAST', class: 'tag-fast', priority: 7 });
            if (leadPct >= 40 || vsSplit <= -0.1) tags.push({ text: 'STARTER', class: 'tag-starter', priority: 6 });

            // 5. Place Stats (only if not a huge winner)
            if (tags.length < 2) {
                if (trkPlace >= 60 && (runner.startsAtTrack || 0) >= 3) tags.push({ text: 'PLACE TRK', class: 'tag-place-trk', priority: 5 });
                else if ((runner.placeRate || 0) * 100 >= 60) tags.push({ text: 'PLACE', class: 'tag-place-dist', priority: 4 });
            }
            
            // Sort by priority and take top 3
            runner.summary = tags.sort((a, b) => b.priority - a.priority).slice(0, 3);

        } else {
            runner.avgTdVsField = undefined;
            runner.avgSplitVsField = undefined;
            runner.summary = [];
        }
    });

    return race.runs;
}

// Render race card
function renderSingleRaceHTML(race, meeting, jurisdiction) {
    const runners = processRaceData(race).sort((a, b) => (a.boxNumber || 99) - (b.boxNumber || 99));
    return `
        <h3>${meeting.trackName} (${jurisdiction}) - Race ${race.raceNumber} (${formatTime(race.raceStart)}): ${race.name || 'Unnamed Race'} - ${race.distance}m</h3>
        <div style="overflow-x:auto;">
        <table>
            <thead>
                <tr>
                    <th>Box</th> <th>Dog Name</th> <th>Last 5</th> <th>Class</th> <th>Style</th> <th>Box Pref</th> 
                    <th>Box Bias %</th> <th>Lead @ 1st %</th> <th>Trainer %</th> <th>Total Runs</th> <th>Win %</th> 
                    <th>Place %</th> <th>Trk Exp</th> <th>Win @ Track</th> <th>Place @ Track</th> <th>Dist Exp</th> 
                    <th>Win @ Dist</th> <th>Place @ Dist</th> 
                    <th>Avg T/D L5</th> <th>Avg T/D vs Field</th> <th>Avg Spl L5</th> <th>Avg Spl vs Field</th>
                    <th>Summary</th>
                </tr>
            </thead>
            <tbody>
                ${runners.map(runner => {
                    const getHighlightClass = (statKey) => runner.rankings && runner.rankings[statKey] ? `top-${runner.rankings[statKey]}` : '';
                    const formatVsField = (value) => {
                        if (value === null || value === undefined) return 'N/A';
                        const className = value < 0 ? 'faster' : 'slower';
                        return `<span class="${className}">${value > 0 ? '+' : ''}${value.toFixed(3)}</span>`;
                    };
                    
                    const isPermScratched = runner.scratched || !runner.boxNumber;
                    const isVisuallyScratched = isPermScratched || runner.isManuallyScratched;
                    const rowClasses = [ isVisuallyScratched ? 'scratched-style' : '', !isPermScratched ? 'clickable-row' : '' ].join(' ');
                    const clickHandler = isPermScratched ? '' : `onclick="toggleScratch('${jurisdiction}', '${meeting.trackCode}', ${race.raceNumber}, ${runner.dogId})"`;
                    
                    const styleClass = runner.runningStyle === 'Early' ? 'style-early' : (runner.runningStyle === 'Mid' ? 'style-mid' : (runner.runningStyle === 'Close' ? 'style-close' : ''));
                    let boxPrefClass = '';
                    if (runner.boxPreference === 'Good') boxPrefClass = 'box-good';
                    else if (runner.boxPreference === 'Poor') boxPrefClass = 'box-poor';
                    else if (runner.boxPreference === 'Neutral') boxPrefClass = 'box-neutral';
                    
                    // Render Summary Tags
                    const summaryHtml = runner.summary ? runner.summary.map(tag => `<span class="tag ${tag.class}">${tag.text}</span>`).join('') : '';

                    return `
                        <tr class="${rowClasses}" ${clickHandler}>
                            <td>${runner.boxNumber || '-'}</td>
                            <td>${runner.dogName}</td>
                            <td>${runner.last5 || 'N/A'}</td>
                            <td>${runner.classChange || 'N/A'}</td>
                            <td class="${styleClass}">${runner.runningStyle || 'N/A'}</td>
                            <td class="${boxPrefClass}">${runner.boxPreference || 'N/A'}</td>
                            <td class="${getHighlightClass('boxWinPercentage')}">${runner.boxWinPercentage !== null ? (runner.boxWinPercentage * 100).toFixed(1) + '%' : 'N/A'}</td>
                            <td class="${getHighlightClass('leadAtFirstBendRate')}">${runner.leadAtFirstBendRate !== null ? (runner.leadAtFirstBendRate * 100).toFixed(0) + '%' : 'N/A'}</td>
                            <td class="${getHighlightClass('trainerStrikeRate')}">${runner.trainerStrikeRate !== null ? (runner.trainerStrikeRate * 100).toFixed(1) + '%' : 'N/A'}</td>
                            <td>${runner.totalStarts ?? 'N/A'}</td>
                            <td class="${getHighlightClass('winRate')}">${runner.winRate !== null ? (runner.winRate * 100).toFixed(1) + '%' : 'N/A'}</td>
                            <td class="${getHighlightClass('placeRate')}">${runner.placeRate !== null ? (runner.placeRate * 100).toFixed(1) + '%' : 'N/A'}</td>
                            <td>${runner.startsAtTrack ?? 'N/A'}</td>
                            <td class="${getHighlightClass('winRateAtTrack')}">${runner.winRateAtTrack !== null ? (runner.winRateAtTrack * 100).toFixed(1) + '%' : 'N/A'}</td>
                            <td class="${getHighlightClass('placeRateAtTrack')}">${runner.placeRateAtTrack !== null ? (runner.placeRateAtTrack * 100).toFixed(1) + '%' : 'N/A'}</td>
                            <td>${runner.startsAtDistance ?? 'N/A'}</td>
                            <td class="${getHighlightClass('winRateAtDistance')}">${runner.winRateAtDistance !== null ? (runner.winRateAtDistance * 100).toFixed(1) + '%' : 'N/A'}</td>
                            <td class="${getHighlightClass('placeRateAtDistance')}">${runner.placeRateAtDistance !== null ? (runner.placeRateAtDistance * 100).toFixed(1) + '%' : 'N/A'}</td>
                            <td class="${getHighlightClass('avgTimeLast5TrackDist')}">${runner.avgTimeLast5TrackDist ? runner.avgTimeLast5TrackDist.toFixed(3) : 'N/A'}</td>
                            <td>${formatVsField(runner.avgTdVsField)}</td>
                            <td class="${getHighlightClass('avgSplitLast5TrackDist')}">${runner.avgSplitLast5TrackDist ? runner.avgSplitLast5TrackDist.toFixed(3) : 'N/A'}</td>
                            <td>${formatVsField(runner.avgSplitVsField)}</td>
                            <td>${summaryHtml}</td>
                        </tr>
                    `;
                }).join('')}
            </tbody>
        </table>
        </div>
    `;
}

function renderSingleMeetingHTML(meeting, jurisdiction) {
    if (!meeting) return `<p>Meeting data not found.</p>`;
    let html = `<h2>${meeting.trackName} (${meeting.trackCode})</h2>`;
    if (meeting.error) {
        html += `<p style="color: red;">${meeting.error}</p>`;
    } else if (meeting.races && meeting.races.length > 0) {
        html += meeting.races.map(race => renderSingleRaceHTML(race, meeting, jurisdiction)).join('');
    } else {
         html += `<p>No races found for this meeting.</p>`;
    }
    return html;
}

function showUpcomingRaces() {
    document.querySelectorAll('#controls button').forEach(b => b.classList.toggle('active', b.textContent === 'Upcoming'));
    venueTabsDiv.style.display = 'none'; 

    const allFutureRaces = [];
    const now = new Date();

    for (const jur in allRacesData) {
        for (const meeting of allRacesData[jur]) {
            if (meeting.races) {
                for (const race of meeting.races) {
                    if (race.raceStart && new Date(race.raceStart) > now) {
                        allFutureRaces.push({
                            jurisdiction: jur,
                            meeting: meeting,
                            race: race
                        });
                    }
                }
            }
        }
    }

    allFutureRaces.sort((a, b) => new Date(a.race.raceStart) - new Date(b.race.raceStart));

    const nextRaces = allFutureRaces.slice(0, 10);

    let html = `<div id="upcoming-controls">
                    <button onclick="showUpcomingRaces()">Refresh Upcoming Races</button>
                </div>`;
    
    if (nextRaces.length === 0) {
        html += `<p>No upcoming races for today.</p>`;
        meetingContentDiv.innerHTML = html;
        return;
    }
    
    html += `<h2>Showing the Next ${nextRaces.length} Upcoming Races</h2>`;

    html += nextRaces.map(item => {
        return renderSingleRaceHTML(item.race, item.meeting, item.jurisdiction);
    }).join('');

    meetingContentDiv.innerHTML = html;
}

function toggleScratch(jurisdiction, trackCode, raceNumber, dogId) {
    try {
        const meeting = allRacesData[jurisdiction].find(m => m.trackCode === trackCode);
        const race = meeting.races.find(r => r.raceNumber === raceNumber);
        const runner = race.runs.find(run => run.dogId === dogId);
        runner.isManuallyScratched = !runner.isManuallyScratched;

        const isUpcomingView = document.querySelector('#controls button.active').textContent === 'Upcoming';
        if (isUpcomingView) {
            showUpcomingRaces(); 
        } else {
            switchVenue(jurisdiction, trackCode); 
        }
    } catch (e) { console.error("Could not toggle scratch state:", e); }
}

function switchVenue(jurisdiction, trackCode) {
    document.querySelectorAll('#venue-tabs button').forEach(b => b.classList.toggle('active', b.dataset.trackcode === trackCode));
    const meetingToShow = allRacesData[jurisdiction].find(m => m.trackCode === trackCode);
    meetingContentDiv.innerHTML = renderSingleMeetingHTML(meetingToShow, jurisdiction);
}

function switchJurisdiction(jurisdiction) {
    document.querySelectorAll('#controls button').forEach(b => b.classList.toggle('active', b.textContent === jurisdiction));
    venueTabsDiv.style.display = 'block'; 
    const meetings = allRacesData[jurisdiction];
    if (!meetings || meetings.length === 0) {
        venueTabsDiv.innerHTML = '';
        meetingContentDiv.innerHTML = `<p>No meetings found for this jurisdiction today.</p>`;
        return;
    }
    venueTabsDiv.innerHTML = meetings.map((m, i) => `<button class="${i === 0 ? 'active' : ''}" data-trackcode="${m.trackCode}" onclick="switchVenue('${jurisdiction}', '${m.trackCode}')">${m.trackCode}</button>`).join('');
    switchVenue(jurisdiction, meetings[0].trackCode);
}

async function loadAllRaces() {
    try {                        
        const response = await fetch('/api/daily-races');
        if (!response.ok) { throw new Error(`Server returned status: ${response.status}`); }
        const result = await response.json();
        allRacesData = result.data;
        console.log(`Loaded races for ${result.date}, computed at ${result.computedAt}`);
        showUpcomingRaces();
    } catch (error) {
        console.error('Failed to load race data:', error);
        meetingContentDiv.innerHTML = `<p style="color: red;">Error: Could not fetch race data. Run 'npm run daily-compute' first.</p>`;
    }
}

loadAllRaces();

let notifEnabled = () => {
  return false;
}

function format_seconds(s) {
  s = Math.floor(s);
  let m = Math.floor(s / 60);
  if (s < 60) return '0:' + ((s < 10) ? '0' + s : String(s));
  s = s % 60;
  s_str = (s < 10) ? '0' + s : String(s);
  if (m < 60) return m + ':' + s_str;
  let h = Math.floor(m / 60);
  m = m % 60;
  m_str = (m < 10) ? '0' + m : String(m);
  return h + ':' + m_str + ':' + s_str;
}

/*********************
 * REACT BOILERPLATE *
 *********************/

const e = React.createElement;
const {useState, useEffect, useLayoutEffect, useRef} = React;

function useInterval(callback, delay) {
  const savedCallback = useRef();

  // Remember the latest callback.
  useEffect(() => {
  savedCallback.current = callback;
  }, [callback]);

  // Set up the interval.
  useEffect(() => {
    function tick() {
      savedCallback.current();
    }
    if (delay !== null) {
      let id = setInterval(tick, delay);
      return () => clearInterval(id);
    }
  }, [delay]);
}

function usePrevious(value, defval) {
  const ref = useRef(defval);
  useEffect(() => {
    ref.current = value;
  });
  return ref.current;
}

/****************
 * LIVE REPORTS *
 ****************/

const CUTOFF = 10;

function Group(props) {
  const ref = useRef();
  const [height, setHeight] = useState(null);
  const [collapsed, setCollapsed] = useState(false);
  useLayoutEffect(() => {
    setHeight(ref.current.getBoundingClientRect().height);
    setCollapsed(true);
  }, []);

  return e('a', {
      className: 'panel-block',
      href: `${props.video_url}&t=${Math.max(Math.floor(props.group[0].relative_timestamp) - 10, 0)}s`,
      target: '_blank',
      onMouseOver: () => setCollapsed(false),
      onMouseOut: () => setCollapsed(true),
    },
    e('article', {className: 'media', style: {width: '100%', borderTop: 'none'}},
      e('figure', {className: 'media-left'},
        e('p', {className: 'title is-5'}, format_seconds(props.group[0].relative_timestamp))
      ),
      e('div', {className: `media-content${collapsed ? ' collapsed' : ''}`, ref: ref, style: {height: collapsed ? null : height}},
        e('table', {className: 'table'},
          e('tbody', null,
            props.group.slice(0, (props.group.length > CUTOFF) ? 9 : CUTOFF).map((m, i) =>
              e('tr', {key: i},
                e('td', null, e('p', null, moment.unix(m.timestamp).format('lll'))),
                e('td', null, e('p', null, m.text)),
              )
            ),
            (props.group.length > CUTOFF)
            ? e('tr', null,
                e('td', {style: {textAlign: 'center'}}, e('p', null, '\u22EE')),
                e('td', {style: {textAlign: 'center'}}, e('p', null, '\u22EE')),
              )
            : null
          )
        )
      )
    )
  );
}

const NOTIF_CUTOFF = 30;

function GroupList(props) {
  const prevGroups = usePrevious(props.info.groups, []);

  useEffect(() => {
    if (!props.info.notify || !notifEnabled()) return;

    const groups = props.info.groups;

    let i = 0;
    while (i < groups.length && i < prevGroups.length && groups[i][0].timestamp === prevGroups[i][0].timestamp) i++;

    const utcNow = Date.now() / 1000;

    groups.slice(i)
      .filter((group) => (utcNow - group[group.length - 1].timestamp) < NOTIF_CUTOFF)
      .forEach((group) => props.onNew(props.info.description, group));
  }, [props.info.groups]);

  if (props.info.groups.length === 0) return null;

  return e('nav', {className: 'panel'},
    e('p', {className: 'panel-heading'}, props.info.description),
    props.info.groups.map((g, i) => e(Group, {key: i, group: g, video_url: props.video_url}))
  );
}

function LiveReport(props) {

  function notify(title, group) {
    let notifText = group.slice(0, 3).map((m) => m.text).join('\n');
    if (group.length > 3) notifText += '...';
    const tag = `${props.info.id}${title}${group[0].text}${group[0].timestamp}`;

    const notif = new Notification(title, {body: notifText, icon: props.info.thumbnail_url, tag: tag});
    notif.addEventListener('click', (e) => {
      e.preventDefault();
      window.open(props.info.url, '_blank');
    });
  }

  return e('article', {className: 'media'},
    e('figure', {className: 'media-left'}, null,
      e('p', {className: 'image is-64x64'},
        e('img', {src: props.info.thumbnail_url})
      )
    ),
    e('div', {className: 'media-content', style: {overflow: 'visible'}},
      e('div', {className: 'content'},
        e('h1', {className: 'title is-4', id: props.info.id},
          e('a', {href: props.info.url, target: '_blank'}, props.info.title)
        ),
        props.info.group_lists.map((gl, i) => e(GroupList, {key: i, info: gl, video_url: props.info.url, onNew: notify}))
      )
    )
  );
}

function ReportApp(props) {
  const [reports, setReports] = useState([]);

  function getReports() {
    fetch(props.endpoint)
      .then((response) => response.json())
      .then((data) => {setReports(data.reports)});
  }

  useEffect(getReports, []);
  useInterval(getReports, props.interval);

  const reportsFiltered = reports.filter((info) => {
    const reportLength = info.group_lists.reduce((prev, gl) => prev + gl.groups.length, 0);
    return reportLength > 0;
  });

  // Display in reverse chronological order, with newest at top
  reportsFiltered.reverse();

  return e(React.Fragment, null,
    reportsFiltered.length > 0
    ? reportsFiltered.map((info, i) =>  e(LiveReport, {key: i, info: info}))
    : e('div', {className: 'notification'},
        e('p', {className: 'subtitle is-5 has-text-centered'}, 'Nothing yet')
      )
  )
}

ReactDOM.render(e(ReportApp, {endpoint: '/_monitor/live.json', interval: 5000}),  document.getElementById('live-root'));
ReactDOM.render(e(ReportApp, {endpoint: '/_monitor/archive.json', interval: 30000}),  document.getElementById('archive-root'));

/*****************
 * NOTIFICATIONS *
 *****************/

if ('Notification' in window) {
  notifEnabled = () => {
    return (Notification.permission === 'granted');
  }

  function checkNotificationPromise() {
    try {
      Notification.requestPermission().then();
    } catch(e) {
      return false;
    }
    return true;
  }

  function askNotificationPermission(callback) {
    // function to actually ask the permissions
    function handlePermission(permission) {
      // Whatever the user answers, we make sure Chrome stores the information
      if(!('permission' in Notification)) {
        Notification.permission = permission;
      }
      if (callback !== null && callback !== undefined) callback();
    }

    if(checkNotificationPromise()) {
      Notification.requestPermission()
      .then((permission) => {
        handlePermission(permission);
      })
    } else {
      Notification.requestPermission(function(permission) {
        handlePermission(permission);
      });
    }
  }

  function NotificationToggle(props) {
    const [enabled, setEnabled] = useState(Notification.permission === 'granted');

    if (enabled) return e('p', {className: 'tag is-medium is-info'}, 'Notifications on');
    return e(
      'button',
      {className: 'button is-light', onClick: () => {askNotificationPermission(() => setEnabled(notifEnabled()))}},
      'Show notifications',
    );
  }

  ReactDOM.render(e(NotificationToggle), document.getElementById('notifications-root'));
}
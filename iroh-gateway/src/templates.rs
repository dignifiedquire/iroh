use lazy_static::lazy_static;
use std::{
	collections::HashMap,
	path::Path,
	ffi::OsStr,
};

pub const DIR_LIST: &str = r#"
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8" />
<meta name="description" content="A directory of content-addressed files hosted on IPFS">
<meta property="og:title" content="Files on IPFS">
<meta property="og:description" content="{{ root_path }}">
<meta property="og:type" content="website">
<meta property="og:image" content="https://gateway.ipfs.io/ipfs/QmSDeYAe9mga6NdTozAZuyGL3Q1XjsLtvX28XFxJH8oPjq">

<meta name="twitter:title" content="{{ root_path }}">
<meta name="twitter:description" content="A directory of files hosted on the distributed, decentralized web using IPFS">
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:image" content="https://gateway.ipfs.io/ipfs/QmSDeYAe9mga6NdTozAZuyGL3Q1XjsLtvX28XFxJH8oPjq">
<meta name="twitter:creator" content="@n0computer">
<meta name="twitter:site" content="@n0computer">

<meta name="image" content="https://gateway.ipfs.io/ipfs/QmSDeYAe9mga6NdTozAZuyGL3Q1XjsLtvX28XFxJH8oPjq">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<link rel="shortcut icon" href="data:image/x-icon;base64,AAABAAEAEBAAAAEAIABoBAAAFgAAACgAAAAQAAAAIAAAAAEAIAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAlo89/56ZQ/8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACUjDu1lo89/6mhTP+zrVP/nplD/5+aRK8AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAHNiIS6Wjz3/ubFY/761W/+vp1D/urRZ/8vDZf/GvmH/nplD/1BNIm8AAAAAAAAAAAAAAAAAAAAAAAAAAJaPPf+knEj/vrVb/761W/++tVv/r6dQ/7q0Wf/Lw2X/y8Nl/8vDZf+tpk7/nplD/wAAAAAAAAAAAAAAAJaPPf+2rVX/vrVb/761W/++tVv/vrVb/6+nUP+6tFn/y8Nl/8vDZf/Lw2X/y8Nl/8G6Xv+emUP/AAAAAAAAAACWjz3/vrVb/761W/++tVv/vrVb/761W/+vp1D/urRZ/8vDZf/Lw2X/y8Nl/8vDZf/Lw2X/nplD/wAAAAAAAAAAlo89/761W/++tVv/vrVb/761W/++tVv/r6dQ/7q0Wf/Lw2X/y8Nl/8vDZf/Lw2X/y8Nl/56ZQ/8AAAAAAAAAAJaPPf++tVv/vrVb/761W/++tVv/vbRa/5aPPf+emUP/y8Nl/8vDZf/Lw2X/y8Nl/8vDZf+emUP/AAAAAAAAAACWjz3/vrVb/761W/++tVv/vrVb/5qTQP+inkb/op5G/6KdRv/Lw2X/y8Nl/8vDZf/Lw2X/nplD/wAAAAAAAAAAlo89/761W/++tVv/sqlS/56ZQ//LxWb/0Mlp/9DJaf/Kw2X/oJtE/7+3XP/Lw2X/y8Nl/56ZQ/8AAAAAAAAAAJaPPf+9tFr/mJE+/7GsUv/Rymr/0cpq/9HKav/Rymr/0cpq/9HKav+xrFL/nplD/8vDZf+emUP/AAAAAAAAAACWjz3/op5G/9HKav/Rymr/0cpq/9HKav/Rymr/0cpq/9HKav/Rymr/0cpq/9HKav+inkb/nplD/wAAAAAAAAAAAAAAAKKeRv+3slb/0cpq/9HKav/Rymr/0cpq/9HKav/Rymr/0cpq/9HKav+1sFX/op5G/wAAAAAAAAAAAAAAAAAAAAAAAAAAop5GUKKeRv/Nxmf/0cpq/9HKav/Rymr/0cpq/83GZ/+inkb/op5GSAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAop5G16KeRv/LxWb/y8Vm/6KeRv+inkaPAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAop5G/6KeRtcAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA/n8AAPgfAADwDwAAwAMAAIABAACAAQAAgAEAAIABAACAAQAAgAEAAIABAACAAQAAwAMAAPAPAAD4HwAA/n8AAA==" />
<link rel="stylesheet" href="/style.css"/>
<link rel="stylesheet" href="/icons.css">
<title>{{ root_path }}</title>
</head>
<body>
  <div id="page-header">
    <div id="page-header-logo" class="ipfs-logo">&nbsp;</div>
    <div id="page-header-menu">
      <div class="menu-item-wide"><a href="https://iroh.computer/docs/ipfs" target="_blank" rel="noopener noreferrer">About IPFS</a></div>
      <div class="menu-item-narrow"><a href="https://iroh.computer/docs/ipfs" target="_blank" rel="noopener noreferrer">About</a></div>
      <div>
        <a href="https://github.com/n0-computer/iroh/issues/new" target="_blank" rel="noopener noreferrer" title="Report a bug">
          <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 18.4 21"><circle cx="7.5" cy="4.8" r="1"/><circle cx="11.1" cy="4.8" r="1"/><path d="M12.7 8.4c-0.5-1.5-1.9-2.5-3.5-2.5 -1.6 0-3 1-3.5 2.5H12.7z"/><path d="M8.5 9.7H5c-0.5 0.8-0.7 1.7-0.7 2.7 0 2.6 1.8 4.8 4.2 5.2V9.7z"/><path d="M13.4 9.7H9.9v7.9c2.4-0.4 4.2-2.5 4.2-5.2C14.1 11.4 13.9 10.5 13.4 9.7z"/><circle cx="15.7" cy="12.9" r="1"/><circle cx="15.1" cy="15.4" r="1"/><circle cx="15.3" cy="10.4" r="1"/><circle cx="2.7" cy="12.9" r="1"/><circle cx="3.3" cy="15.4" r="1"/><circle cx="3.1" cy="10.4" r="1"/></svg>
        </a>
      </div>
    </div>
  </div>
  <div id="content">
    <div id="content-header" class="d-flex flex-wrap">
        <div>
            <strong>
                Index of
                {{#each breadcrumbs ~}}
                /{{#if this.path }}<a href="{{ public_url_base }}{{ this.path }}">{{ this.name }}</a>{{else}}{{ this.name }}{{/if}}
                {{~ else }}
                {{ path }}
                {{/each }}
            </strong>
            {{#if root_cid }}
            <div class="ipfs-cid" translate="no">
            {{ root_cid }}
            </div>
            {{/if }}
        </div>
        {{#if size }}
        <div class="no-linebreak flex-shrink-1 ml-auto">
            <strong>&nbsp;{{ size }}</strong>
        </div>
        {{/if }}
    </div>
    <div class="table-responsive">
    <table>
      {{#each links}}
      <tr>
        <td class="type-icon">
          <div class="{{ this.icon }}">&nbsp;</div>
        </td>
        <td>
          <a href="{{ this.path }}">{{ this.name }}</a>
        </td>
        <td class="no-linebreak">
        </td>
        <td class="no-linebreak">{{ this.size }}</td>
      </tr>
      {{/each }}
    </table>
    </div>
  </div>
</body>
</html>
"#;

pub const NOT_FOUND: &str = "
<div>
    <h1>404 Not Found</h1>
    <p>
        The requested resource was not found.
    </p>
</div>
";

pub const STYLESHEET_TEMPLATE: &str = r##"
body {
	color:#34373f;
	font-family:"Helvetica Neue", Helvetica, Arial, sans-serif;
	font-size:14px;
	line-height:1.43;
	margin:0;
	word-break:break-all;
	-webkit-text-size-adjust:100%;
	-ms-text-size-adjust:100%;
	-webkit-tap-highlight-color:transparent
}

a {
	color:#117eb3;
	text-decoration:none
}

a:hover {
	color:#00b0e9;
	text-decoration:underline
}

a:active,
a:visited {
	color:#00b0e9
}

strong {
	font-weight:700
}

table {
	border-collapse:collapse;
	border-spacing:0;
	max-width:100%;
	width:100%
}

tr:first-child td {
	border-top:0
}

tr:nth-of-type(even) {
	background-color:#f7f8fa
}

td {
	border-top:1px solid #d9dbe2;
	padding:.65em;
	vertical-align:top
}

#page-header {
	align-items:center;
	background:#0b3a53;
	border-bottom:4px solid #69c4cd;
	color:#fff;
	display:flex;
	font-size:1.12em;
	font-weight:500;
	justify-content:space-between;
	padding:0 1em
}

#page-header a {
	color:#69c4cd
}

#page-header a:active {
	color:#9ad4db
}

#page-header a:hover {
	color:#fff
}

#page-header-logo {
	height:2.25em;
	margin:.7em .7em .7em 0;
	width:7.15em
}

#page-header-menu {
	align-items:center;
	display:flex;
	margin:.65em 0
}

#page-header-menu div {
	margin:0 .6em
}

#page-header-menu div:last-child {
	margin:0 0 0 .6em
}

#page-header-menu svg {
	fill:#69c4cd;
	height:1.8em;
	margin-top:.125em
}

#page-header-menu svg:hover {
	fill:#fff
}

.menu-item-narrow {
	display:none
}

#content {
	border:1px solid #d9dbe2;
	margin:1em
}

#content-header {
	background-color:#edf0f4;
	border-bottom:1px solid #d9dbe2;
	padding:.7em 1em
}

.type-icon,
.type-icon>* {
	width:1.15em
}

.no-linebreak {
	white-space:nowrap
}

.icon-cid {
	color:#7f8491;
	font-family:monospace
}

@media only screen and (max-width:500px) {
	.menu-item-narrow {
		display:inline
	}
	.menu-item-wide {
		display:none
	}
}

@media print {
	#page-header {
		display:none
	}
	#content-header,
	.icon-cid,
	body {
		color:#000
	}
	#content-header {
		border-bottom:1px solid #000
	}
	#content {
		border:1px solid #000
	}
	a,
	a:visited {
		color:#000;
		text-decoration:underline
	}
	a[href]:after {
		content:" (" attr(href) ")"
	}
	tr {
		page-break-inside:avoid
	}
	tr:nth-of-type(even) {
		background-color:transparent
	}
	td {
		border-top:1px solid #000
	}
}

@-ms-viewport {
	width:device-width
}

.d-flex {
	display:flex
}

.flex-wrap {
	flex-flow:wrap
}

.flex-shrink-1 {
	flex-shrink:1
}

.ml-auto {
	margin-left:auto
}

.table-responsive {
	display:block;
	width:100%;
	overflow-x:auto;
	-webkit-overflow-scrolling:touch
}
"##;

lazy_static! {
	static ref KNOWN_ICONS: HashMap<&'static str, ()> = HashMap::from([
		(".aac", ()),
		(".aiff", ()),
		(".ai", ()),
		(".avi", ()),
		(".bmp", ()),
		(".c", ()),
		(".cpp", ()),
		(".css", ()),
		(".dat", ()),
		(".dmg", ()),
		(".doc", ()),
		(".dotx", ()),
		(".dwg", ()),
		(".dxf", ()),
		(".eps", ()),
		(".exe", ()),
		(".flv", ()),
		(".gif", ()),
		(".h", ()),
		(".hpp", ()),
		(".html", ()),
		(".ics", ()),
		(".iso", ()),
		(".java", ()),
		(".jpg", ()),
		(".jpeg", ()),
		(".js", ()),
		(".key", ()),
		(".less", ()),
		(".mid", ()),
		(".mkv", ()),
		(".mov", ()),
		(".mp3", ()),
		(".mp4", ()),
		(".mpg", ()),
		(".odf", ()),
		(".ods", ()),
		(".odt", ()),
		(".otp", ()),
		(".ots", ()),
		(".ott", ()),
		(".pdf", ()),
		(".php", ()),
		(".png", ()),
		(".ppt", ()),
		(".psd", ()),
		(".py", ()),
		(".qt", ()),
		(".rar", ()),
		(".rb", ()),
		(".rtf", ()),
		(".sass", ()),
		(".scss", ()),
		(".sql", ()),
		(".tga", ()),
		(".tgz", ()),
		(".tiff", ()),
		(".txt", ()),
		(".wav", ()),
		(".wmv", ()),
		(".xls", ()),
		(".xlsx", ()),
		(".xml", ()),
		(".yml", ()),
		(".zip", ()),
	]);
}

pub fn icon_class_name(path: &str) -> String {
	let ext = Path::new(path)
		.extension()
		.and_then(OsStr::to_str)
		.unwrap_or("");

	if KNOWN_ICONS.contains_key(ext) {
		return format!("icon-{}", ext);
	}
	"icon-_blank".to_string()
}


pub const ICONS_STLESHEET_TEMPLATE: &str = r##"
/* Source - fileicons.org */

.icon-_blank {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAWBJREFUeNqEUj1LxEAQnd1MVA4lyIEWx6UIKEGUExGsbC3tLfwJ/hT/g7VlCnubqxXBwg/Q4hQP/LhKL5nZuBsvuGfW5MGyuzM7jzdvVuR5DgYnZ+f99ai7Vt5t9K9unu4HLweI3qWYxI6PDosdy0fhcntxO44CcOBzPA7mfEyuHwf7ntQk4jcnywOxIlfxOCNYaLVgb6cXbkTdhJXq2SIlNMC0xIqhHczDbi8OVzpLSUa0WebRfmigLHqj1EcPZnwf7gbDIrYVRyEinurj6jTBHyI7pqVrFQqEbt6TEmZ9v1NRAJNC1xTYxIQh/MmRUlmFQE3qWOW1nqB2TWk1/3tgJV0waVvkFIEeZbHq4ElyKzAmEXOx6gnEVJuWBzmkRJBRPYGZBDsVaOlpSgVJE2yVaAe/0kx/3azBRO0VsbMFZE3CDSZKweZfYIVg+DZ6v7h9GDVOwZPw/PoxKu/fAgwALbDAXf7DdQkAAAAASUVORK5CYII=);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-_page {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAmhJREFUeNpsUztv01AYPfdhOy/XTZ80VV1VoCqlA2zQqUgwMEErWBALv4GJDfEDmOEHsFTqVCTExAiiSI2QEKJKESVFFBWo04TESRzfy2c7LY/kLtf2d8+555zvM9NaI1ora5svby9OnbUEBxgDlIKiWjXQeLy19/X17sEtcPY2rtHS96/Hu0RvXXLz+cUzM87zShsI29DpHCYt4E6Box4IZzTnbDx7V74GjhOSfwgE0H2638K9h08A3iHGVbjTw7g6YmAyw/BgecHNGGJjvfQhIfmfIFDAXJpjuugi7djIFVI4P0plctgJQ0xnFe5eOO02OwEp2VkhSCnC8WOCdqgwnzFx4/IyppwRVN+XYXsecqZA1pB48ekAnw9/4GZx3L04N/GoTwEjX4cNH5vlPfjtAIYp8cWrQutxrC5Mod3VsXVTMFSqtaE+gl9dhaUxE2tXZiF7nYiiatJ3v5s8R/1yOCNLOuwjkELiTbmC9dJHpIaGASsDkoFQGJQwHWMcHWJYOmUj1OjvQotuytt5nHMLEGkCyx6QU384jwkUAd2sxJbS/QShZtg/8rHzzQOzSaFhxQrA6YgQMQHojCUlgnCAAvKFBoXXaHfArSCZDE0gyWJgFIKmvUFKO4MUNIk2a4+hODtDUVuJ/J732AKS6ZtImdTyAQQB3bZN8l9t75IFh0JMUdVKsohsUPqRgnka0tYgggYpCHkKGTsHI5NOMojB4iTICCepvX53AIEfQta1iUCmoTiBmdEri2RgddKFhuJoqb/af/yw/d3zTNM6UkaOfis62aUgddAbnz+rXuPY+Vnzjt9/CzAAbmLjCrfBiRgAAAAASUVORK5CYII=);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-aac {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAnhJREFUeNp0Uk1PE0EYftruVlvAUkhVEPoBcsEoLRJBY01MPHjCs3cvogcT/4qJJN5NvHhoohcOnPw4YEGIkCh+oLGBKm3Z7nZ3dme2vjOhTcjiJJvZzPvOM8/HG2q325Dr3kLp7Y1ibpIxjs4KhQBZfvV6s7K5Vb0bjeof5ZlcGysP1a51mifODybvzE8mzCbrAoTDIThMoGXZiZ4YSiurf+Z1XeuCqJ7Oj+sK3jQcNAmg8xkGQ71mYejcAB49vpmeuzJccl0+dUj6KIAvfHCPg3N+uAv4vg9BOxcCmfEzuP/genpmeqhEMgude10Jwm+DuUIyUdTlqu2byoMfX/dRermBeExHsTiWNi3+lMpzRwDki8zxCIATmzbevfmClukiP5NFhJgwkjeRTeLShdOoVJqnAgwkgCAZ6+UdLC9twjQZ8pdzioFkZBHY3q6B3l4dJEEEPOCeD4cYVH7Xsf15F+FImC775INAJBJSkVoWo0QY9YqgiR4ZZzRaGBkdwK3bFxGLRZUfB3Rm2x4x9CGtsUxH9QYkKICDFuLxKAozGZwdTqBRs2FbLlXbiPdECMCHadj/AaDXZNFqedCIvnRcS4UpRo7+hC5zUmw8Ope9wUFinvpmZ7NKt2RTmB4hKZo6n8qP4Oq1HBkKlVYAQBrUlziB0XQSif4YmQhksgNIJk9iaLhPaV9b/Um+uJSCdzyDbGZQRSkvjo+n4JNxubGUSsCj+ZCpODYjkGMAND2k7exUsfhkCd+29yguB88Wl7FW/o6tT7/gcXqAgGv7hhx1LWBireHVn79YP6ChQ3njb/eFlfWqGqT3H3ZlGIhGI2i2UO/U/wkwAAmoalcxlNA1AAAAAElFTkSuQmCC);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-ai {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAk5JREFUeNpsU01vElEUPTPzZqBAQaSFQiJYUmlKYhoTF41L3Tbu/Q/+AvsX3Bp/gPsuWLrqyqQ7TUxMtAvF1tYGoXwNw7wv7zwYgtKX3Lw379575p5z77O01ohW+/DVh8zj7aYKhflGdG9ZsGwLNydffgVfr19YHvsEa+Zu/nxndob5StQK+dyzvZzyw/gKlmMj7IygFM+xvNcanp4/t5dAomXHBy2UUBOO2MAl/B9/cPb6PULuoHx0WM0e3GvpUOxD3wZAJWutZqYUYmqpSg5OMgH3YQObL59W0/ullpryR3HegkKEqiWBSGV4R3vQ7sIhScTZFTpHx3A215B5sluVY/WWMg7+ATB/lcLsKpTonHzD+OMFEuTz8ikkt9Kwt9YJZB38cpBdoQAZJdLvCGByfoPB6Xdk90pYy6Xg3c/DaWwArg09DaG5lCsUFN0pckZAojdC8m4auBqaALuSgez7VB1RtDSUWOQvUaBLFUzJBMJ2DwmPgd1Jwm0WoSgJfjDvrTKxtwAIyEkAOQ5hU//Zdg5uowDlUNMnwZLW0sSuUuACYhwQRwFvJxupCjEYUUccOkoaKmdOlZnY1TkgAcXAhxhOwLsDsHoN3u4O5JTDfVCH6I9nfjId3gIgSUATFJk/hVevGtOMwS0XwQ3AzB/FrlKg8Q27I2javVoZrFgwD4qVipAEyMlnaFArzaj/D0DiMXlJAFQyK2r8fnMMRZp4lQ1MaSL5tU/1kqAkMCh2tYI+7+kh70cjPbr4bEZ51jZr8TJnB9PJXpz3V4ABAPOQVJn2Q60GAAAAAElFTkSuQmCC);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-aiff {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAohJREFUeNpkU9tqE1EUXZmZpE3aTBLbJFPTtFURtSCthr7UCyKKFJ/9An3og6Ag/oXfoUj7og9asCBYKT6UIPHaWtpq7NU2aZK5z5wZ9xxMpMwZDuewz9prr32ZiO/7CNaDx3OLt6fOjBqGg/aKRCIInp8+KzfKH7fudnVF58nE16el+/yU2mBFSWZKpWJKVc0OgUBo02K4NDmU6o75Mx+Wdu9IUXFeiOA/pn1xHeYaugVDdzpbp91qGlAKGTx8dC19/Wpxhjnsxj/RRwk85hGJC9d1O6fneWAuoztDYSSLe9OT6SuXB2ccx73Z9uukwDwfls1g0xZIY/Ad/Gnyt/XVfbyYrSDRE8PExHB6/8B6QuaxIwRBFMt0iIAiMx+LCys8jfGJEUik2WpZOD2SQf9oDtVqQwopCAiY66FS/om3b75CVS2MlU7AJ2WiJBCZjZ2dJuRkDJZFwFAR7UCBja3fNfxY2YEoCtRCj9em3Tpds6FpJseGCBxS0GgYGBzqw62p84gnYnAI2CSbSbPhEpFAaE2zODaUAlWWwDoS5DheGqbWpVE/0CmqCY9qkEyINBceb2uADRNQ8bSWAVVzIFKomCQim+0luS4yKYlsHlRyZo7EsSEC23K5vAsXh/H92zZkuRvxeBS5nEx2yp2KqhxPoV5TYS/8CtdApylM9sZQKKSQzyeRTseRV2QoAzIYY8jme5DN9fI0dQoUIjANGydP9VM7PZw9p/AiBpNYrdbw/t0yTJqRtdU9UrfJCUMpSJIgbWzsYe51BcViHzLHeqCRqhZ1YX1tFwNfZBxS9O3NWkAcHqR606k/n/3coKAoV/Y7vQ/OYCZevlrmv3c0GsFh06u3/f4KMABvSWfDHmbK2gAAAABJRU5ErkJggg==);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-avi {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAm1JREFUeNpsU8tu00AUPXZcN0nzTpq2KQ3pAwkIAnWHqCoeexBb+AQ+ABZ8A2s+AIkdm266QUJIFWKBkHg1KpRHi5omJGkbJ3bGHj+4M1EQrTvSyGPPueeec++1EgQBxHp+/9mbyuriRZdxjJaiKBD3W+u1+p9a856max+gDO8ebT+WT20Ezi9NZi/crqadvn2MQBAGfpCOpqNru2937vxPIpY6Onjccx3Twck9MBiSU0ncfHirXFmZX3Md9wqCUwiEVN/zaQfHt0vfbBe5uQyuPVgpl5Zn11ybL4/i/lkICOw5niQRGQShoiqI6Bo43W2ub8n3hRtLZT7gTynk6gkCX9gAOxpAnxhHZDwC1/aI1EViJolu/QhKRMHZ1UX0Gr1USIEn5FPWHy+/wTokkrQOq2vBaHZBN4hmY9Jwfr4An/teiEB45ZZDwDiMhoExT0N+sYDCuUkkplLIlXP4/XEXdo+RUhdhBSSfUwtVTUG8MIHK9QVqI7D/uY6vr2pwmCPrkz+Tk9gwARWQ9WxppbXZhNnpw+ya4A5HZi6L4lIR8WyCcL6sTZiAWjWgAmpxkn5+kqTamK6WkCwmERmLDLvjB0ML9ikWXPLFuozYOap3L8HYN6DHdbS/d5CeTVBndBz87FCBLYkNTyIjBQemnIEsSY5lYrK1+UoWcToLMjEHAyIQ2BCBSx/NVh+ZUhrqmEqBebS3WyhdLg0zt/ugAaIklsSGLHCLa6zDMGhZ2HjyGsnpFPqNHnY2fmHv3R5SMymYbROszSQ2ROAY9qHiofvlxSc5xsKKqqnY3diRE9h4X5d/pzg7lnM4ivsrwADe9Wg/CQJgFAAAAABJRU5ErkJggg==);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-bmp {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAmZJREFUeNp0U+1rUlEY/13v9YV0vq2wttI5CdpL9aEGBZUDv0df668I6n+or0UQ/RuuD0EgVDAZrsKF4AR1a6COKW5qXvXec27PuVeda3bgcF6e8/ye5/d7niMZhgExnK9fbTrm5pbBGMZDkgCyq+VyhTUaT6Eo2ZHJePPWXJXRhez3B1yxmM/QdctXUSCgtV4Py4CvY3cky4e1x5DlLCaGbbzjXDcousG5OQe5HPRSCQPK4PpsEM/XH4WvhS4noeu3JwHGGRiULhsMoKZS4I0GtEIB9mgULJGA0+9DPBpBT7sffvf1W/Lg6OgJufw8C0CRGEXWazUwiiyFQjA8bsjVKjaJzovMD/Q5gxyJhG2cvyeXe2cAuADQNGBmBvLaGuTFRaDfh31lBTWi9pumjbK0B4JQul3vOQpM8JdskOLrdCvDcDjAsjtg5TIkoiKLaokMNR2cnZbqNAMycqG7XbHKR2fMzwO/dsxSwu0BiBJsNsv2LwAJAJCI5ux2gXYbqNetcz5PoORI1cDS0n8AxGW7A+zvEYBKZ2ZlcsEtJLbedMjePBaCTQMghx45ulyWkzxMVUQ2RMQhLfFO16YAqCrixPnm6iqKrRb2W23EfF4cUNSrHg90cr7hDyB33MTnSmUKALVs4uIlROjxg+AsPhGVl3fuIl2tIOB0Ya91gkOi9mxhAal0ekork1ic/kGLBORMxy2K1qS9V1ZQbNThIj2EGh+2tsyOnSai8r1UxMNIBB+LRTTULr4Uds0K1tU/uOLxIrmbNz8XXSrnASSpubG9fbKRyVh1n/zSw29t9oC1b47MfwUYAAUsLiWr4QUJAAAAAElFTkSuQmCC);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-c {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAcxJREFUeNqEUk1rE0EYfmZnkgoJCaGNCehuJTalhJZSUZB66a0HwXsP/Qn+FM+9+hty0LNYCr2I7UVLIW0Fc0hpQpSS7O7MrO9MspuvVV8YMnk/nn2e5x0WRRFMvP/w6WSz5jbi/9NxfP693Wp3DrJCnMW5d28P7a+IE15lufR8o1ZEStwPhkWHsWbrZ+eNEPxsuubEF6m0TBv2Q4liPofXuzveulttSqW2UwH+GjqC0horpSL2njU89+FyMwjlTlxOJMTa9ZQHzDQIjgwdom9zLzfXPc75kbnOAswBJTlC2XrqQRMLxhi442DgB4UFBhgPpm3B5pgBHNUUxQKAHs8pHf3TEuFMetM9IKr/i2mWMwC0SnuSFTG2YKyppwKYVdGO7TFhzBqGIenVeLCUtfURgErucx5ECKREKBU4d3B718PHz6cICGT/1Qs8qpQtGOdyhtGEARWDQFqQJSeDL98u4VbLaKw9IRAJPwjtoJGlVAoDQ800+fRFTTYXcjlcXN2g++s36p5Lzzlve1iEROa8BGH1EbrSAeqrjxEqicHQt8/YSDHMpaNs7wJAp9vvfb287idboAVkRAa5fBYXP9rxO4Mgf0xvPPdHgAEA8OoGd40i1j0AAAAASUVORK5CYII=);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-cpp {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAfJJREFUeNqEUs9PE0EU/mZ2WgqpXX+QIDFdalVslh8NlAOQaOKFAwfvHvwT/FM8e/U/MOnBmwcj8WD0ACEGghIkbU0baaEthe3OTJ0ZWV26q37JZt68ee/b9733yGAwgMbL12/fz+azbnAPY2Nrt7Zfqz9JMrYZ+J4/e2pOFjiciRvXlgp5GzHonXk2o6S8V6k/TjBrM/xGA4MLyeOSPZ8jkx7D+uqCU3Amy1yIYizB36AlCSkwfjWDR4uu40yMl/s+XwjeWThQQ4Z6QNSnSkYykcDXasP4lmfvOZTSF9q8TDBEFPbN5bOqCglCCCxK0TvvZyIV4CIxbgpC+4gm/PUmFCIE8iJPyME/e8Lon9j4HvyHYLjKSwRCSEUgf9+15mFbx8QS6CZJMzJ9SlBCwX3fJDLG4PX7ykcwkmQmJtpEhWa7g1dvNlSwjwelebz7tAXLolh0p/Fxe9fErK2WDFGEgKjxfNjegX0lDTc/heNuF99/HGEslcKXwyoazWNDdlCr6+DoJgrBzdI0T9rYO6yg2zszMlaKM3Dv5OBzbuyZuzm1B16U4Nzz2f3cFOx0Gq12F9cztpExncsqYoaHpSIKtx0zJdVIFpHQ6py29muNk1uTN829o/6SHEnh80HFaE6NjmLnWxUJy1LyTltB3k8BBgBeEeQTiWRskAAAAABJRU5ErkJggg==);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-css {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAk1JREFUeNpsUktvUlEQ/u5DoCLl/RAKKKUvWmIxjYntQtcu3LvwJ/hTXLt16coFC2PsojEaMKZtCqFaTdGmjbS0CG3By+vei3OOBSGXSU7uzNyZ78z3zRF6vR6YvXzzPrMUCyf68bB9zO+VfpROn5hkOdfPPX/2lH/lfiLidztX5mN2jLGG0rKLENIE8liWpdzwP7HvqJqujmvudFU4bFY8Wk1FZsOBtKppd8YCDNu77CZevd3gflfTUFcUhP0ePLibiIR9rjSBpgwAfe4dVcV6dhtep4PH5msylGYLrzeybErcT85FYiH/CyPAf74gObC2vMhzsiRhPhpC6eQUM+EA1pJzILEnjRSuJsju7MJqsUCSRei6Dp3yXqcdGlHZ/rLPazQWGCn8+6YW4pAkEW0SjzUzanWlCa/LgcR0lNfovTEi6lcIkzesnM/R8RlN0INGp3h4DHoDsE5YRvQyiKiRSMzikRAOS2WoqoZWu41K7RwzlOOAVDMMMHhIGvFlRxJFrKYW0ep0IYgC3SDh4b1lTJjNfENsrazOAMAw680mPuW+8lFno1P4XDigRhOiwQAyJK7TbsNS/PaA7giAIAhYz2yRgBIfsVA8wIetPG6FAqhdNrC5u0f+TUyHgyMTDDToEt/ftQsEvW4EPG5OZcrvw0mlimarTXkPfpXPcNlQoGtjACgpryQXsPNtH/nvRXqBJpoKHMzGNkNB0Odls7LNyAYKpUq1dt1iuvB7fRDp9kr9D1xOFwkpoksXusmXaZWFn0coV89r/b6/AgwAkUENaQaRxswAAAAASUVORK5CYII=);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-dat {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAfVJREFUeNqMU01PE1EUPe/Na0uptmlASg3MoiZgCA3hQ8PHAjbqwsS9C3+CP8W1W/+BSReyYUPwI4QAVkAgUEgIbVIg1FZb2pl5b3zv2cHBjsaTTOa+e989OffcGeK6LhTevFv+OJoZHPHOfrz/sl86KpWfhxnLe7lXL1/oN/MSZqonOXU/k0AA6lfNhEFIrlAsP2PMyPtr1AscLpyg5pbtIHErhqez4+awmc45nI8FEvwNaiQuBHqTcSxMjJhmX0/Osp1xr878FxWEzwMinxAzEA4xFIpnOjedHTKpYbxW4U2CP4j8uWxmUKsghMCgFI2mFe9QgHZj0Ba4yhFF+KvGJToIRLuPC/efnjD6+26wB1Lq/xgbSCBXKeWJG/OTdky8cWTdT3C9RmWSGk2XCLlWo4xTNbfN5qh7PpXM72GjZeHt0gpq9QbmH4whGb+NpU/reDQ7hcWVVXxvXOHxzCQopQEKXKEbL6o1ZIcy+LC5g62DY2zsHeC0fA4zndIrHOjvg2XbAQRSfsuy9XxC2qzi/H5B6/68W0AsGkW0KyJPBLbDO0fg3JX/CUM81i0bD6WKe6j9qOPJ3EMcF0tSNsFA6g6alqW+VtZBUL78Vtk+Oqne7U9rs5qOQCjSheJFBeFIFOfVujSUYu3rIc4uqxWv76cAAwCwbvRb3SgYxQAAAABJRU5ErkJggg==);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-dmg {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAn9JREFUeNpsU01rE1EUPe9lkk47yWTStCmtNhFSWxos2EXVhSsRcasuxYV05V8Qf4DgD/AvCK5EV1oFI7iUBqmCNdDvppq2mWSSzEzy3vPOpFFq+uDNfR/3nnvueXeYUgrBWH1/9/NE7k5BKRnuRcfF2qdnmJq9DeF9tQ+2isuMsxXGWHh/a1mEVsPJSI5fSU3OPEj291IIlN49RXz0KqzEQjIeZS/L5Y/3wPGhDxIM/i/A7fZWgVG0t5EaG0ZUa0JGM8gvPrZmLt58QYwv91mfAqCIE0sAqgumBFITGQzpUYhuF0KfRa7waDyXXXolpVrsh/0tgSLDr5I+wUZo1UHCSkAficPzY6juFSmbRPrC/azjq+fkcO00gAqoU7B0ETKkfWbuCTjTYeq5oESAauexcTScX+ZACWFm0YQSLZKhHdr67+/wW0e0dgjYo3sCEXXybYtBDVSHLp2es3IpsILS24c42lkBg6DzRjgRzCDZ/xr0GNRJwwYiWgzt+hYMawleu0V3wbkT+kUirOc7IGJAz68R/Qak1BAlx3hqASPGBJRXpXOv58dkz3eAgQoOm4hyj57NgZm0MHvpBmK6QdUdg/DAg9cRkhicBSDaKJdeo1bdxmR2DtWDDUxl51HZ+QHTysD3XdQO95Gfv06aeGcAdBrY3Chi8lwO3768QWX7J5q1XWyVSxgajiOXLyBG2hzurRKV9lmt7ISNkkjo6HhNyjoK+2gXRsKE57ZIE2ot10Z1fz0Ue4ABVw3NMjnW14rInh8jTYywoTg3EOFpOM4mXNfH9PQUfGlrAwBOs3I8ljbtuMWhRWzIIPrkn+GcYcgIWEowbZ+0qB334/4IMADESjqbnHbH0gAAAABJRU5ErkJggg==);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-doc {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAppJREFUeNpsU79PFEEU/mZ39vZu77g7DokcP04BBSUmiEKCSCxs7Ei00JAYO2NlTKyMrX+CJhaGwopSQ0dMtFEsbDRBgiZEQIF4IHcg+2t2Z8eZ5QDlnM1mZ9+8973vfe8NEUJArfSNhzPG0VIfeIiDRSDkw1cWVt3N8rhG6SdSO2Gvn8dfuueqZwuNZqk3Jxg7iNcIfBbgXD6ZC8u5qffzX8eoYeyDxC77uygKhcouovgVUQj1H4YB2ovNuD9+tTTU0zMVBmG/+C8AIYh8F361DL/yE5HnADKYlVdg6MDAmW7cuz5WGuw+PsWDYGAvbL8ECFUt4K7/AHd/I9c7BLaxinD2Ld5Zo7g78RLuRhlBS2cpWbGfStfhfwCEpK0nUjCbWuGsLciSOELPhkq/YgdY3l6HsLfRcLYf+pHNbH0JigEPkLAyMsiEJ7NrqQzM1i7wyhoMZqOhvQs6Z0ovXgdAJACRoulEg5HOwrOroKk0zOY2BDtVpTF0CU6kLkQJXa+BNEoG0lMSsBBKQXWNQktmoGcaYeSaQCIVWOvUYQAiWZFQtk5mSMoSzEILtBrTfEcviC5bwVwQmoh96wA0ic5dB57ngeoaTIPCdb34zDITYNLOOIeVSsW+dQC+7+NSWx6jJ4tY/rWNV7PfcGv0tBoPTM7M4eKJVgx2FTE9u4QPS6x+kHzfw/mOAjarW2hJG3hy8zIceweuY+PRtREMdzbjzcd5WBqPB6xeRGUMGRzHjWvMmxQ7tiOF1JBN6FiTd6Sy9RuFbHpX7MMMqOD088Ii+op5OUAO7jyeRGfBwrF8Cg8mXuDL4neMXzgFwhwZz+hf7a9d5yu3Z6DTPjVQIY9k7erO7Y63Lvc8ErEeyq6JaM6efjai4v4IMABI0DEPqPKkigAAAABJRU5ErkJggg==);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-dotx {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAndJREFUeNpsU01rE1EUPTPzJk0y+WhMStW2qdVWxUVEQUF0I+4ELQiC7lz4N9z0T+hG9wrdZKUgLqulhrbSag1CKpT0g7RpYjqZmffle5NEKdMHlzfvvXvPPffcO4aUEno9f3Vt4dTp+BXOe+fB0u/NbVpv7h89NU1j1TCM8H7+xY9wJwPHZMbOjRadLAvE/2gToJTiTPx89k+OlVd/LT+0TPIPpO/SzyQk40xCMxBSZ9Z3CoAx5DOjeHT7SbE0XSpzwa8OWB9jINELolQg8AR0EgUKn1PIlIWpkUt4cPNxkTOU12trs8p95RiAXpqaztqou8q6SKQJJmZSqGwsodFsIJk1kcyLYv7IeafcLx4HUNkFF4jFTExMZ0B9DrfD4HUEusYhWs4GPEJg5wly/tBYRIOeDhpEwlS34xcyajdQr3UwOT2MlJOEBRuGNHWp9AQRVXDfQiFV/U5GBSiQ5p6ngBEa5z3fiIhC6g6IMDBwOdoHPkYnHPVyhN0tF7E4QSpr94CEOKELffq+y9Bq+DCJ7rWBoQQBVbPR2O6G4OlsLASJMtCZfQqm0NP5IVWnamdAkUxbyuIYtD7wWegb0YAzAVMkkI6NwPM9xEwHloyDGAmk7AKS9rAS0FKOdugbYeAHPu7OPEM+MY7q3hIKqTFQHmC3XcONc/fxdfMDrk/ew/edzyhvvTmBAddocVRqH3Frahau56qpZDho7+PnTgXffi/gbHYmLEvPSIQBp5JU62sYz13G609zKBXvoOMdYn2zgm7Xg2MVML/4Eu3uPgxhk2gXmNl8v/i2pcXTP8tKdTEcbWLZqDQXwu/l6pfwbEnSGsT9FWAA4mdHv2/9YJ4AAAAASUVORK5CYII=);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-dwg {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAoFJREFUeNpsU0tPE2EUPfOg006hD4rQh8WgbCSwkKgbF2owujaCiQsXxpX+D6MmbtXEsHCLmIAbE6NLo8YlGIxREIshIqVl+mQ6j8/zFVCb4UtuZua795577rl3FCEE5Bl79vPd5LHYiOP7cH1AUWi85ytmvlas1bJ9E5ryBntH3BpuP/X9i7ovkluuiE8N9SDepaLpCcRCCqa/VDCaMuIjSWP25Upl6n+QDoCz6Yh7KKzh3sI2LuUimPtRRyaqodj0MDloYiITSTi+mH29Wu0AUf9CsZPJoW5czJl48LmCc5kIKo5Al67B9gUGYxrun+5NnMlFZ+GKiQADj2a7AquseLIvjMv5KMaSBu4sWVir+3i8VIVKYSby0UTdFU8Znu8AYBHQgVOJEN5uOXi4UsdawwU0FSf6TaSoyw6DRvukPkgGWpDKy4F8a3jImCrqFDFn6rhKPR4VGnhvOTAY3WLcjifcQAsqRfhUc/Gq1MKNbBh9nIAMDjEppocxs9HCMktfGTCwP/oOBkUKNk/qF3pDYC6Ktk8RfWzyaaoKrqdDaBDwya8W1m0/CPCR3kFy7CcnmWQRUJqcRJFUKtTnPCeR71LwoeYF92CYyVnCFZpCTrRtCv5to2St8SOrKxiPqEEA4fkYT+mI0rdoeUiH1XZVuQPpsIKqw2QmfifTsnOABiWySlH9uU0Hh2MqjsZV5LtpPSoGeN9rKnhBX7ehoOSLIIPfnGONXGMMWN7xUfVldYDbjM3mrh5HCDgS17DhHgDQcIU+XbBxnDTn1x1UuQcJ9iv7l5Q5e1zLGri92EDJFnoAgHtcfr6wbbVXUqq193+0z97n3UJt1+d51n7aHwEGAAHXJoAuZNlzAAAAAElFTkSuQmCC);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-dxf {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAo5JREFUeNpsU0trE1EYPfNMmtdoH2kDNmJbaVFcaBVFpAsREQpFwY0bu3HjQnTj1mVd+ANcuC3qQixmry6E0kWFVIQ+bKy2tbFJm3emyXTujGca+4DkwsedfLnn3POd77uS67rw1vC79ek7fZEzpu3AYUqS9tKQGZPLpa3VXP0uFCmJ/8t9OLC3q/uJbcs5bkIybvdHoMsSbLKENRmvU2WcNnTjRFD7ML1WGSPJHI6sA4KRWMAWVDPxLYex3iCmfpuIh1QsFSyMxQO4GvXHHwOJ6XWSyIck8v6HQsnjAxFc7vTj2VwBg4aG78VdBHQFCk+dbVcxMdwev9gTSEC455sIBOu2KLsoJFzqasP9vjCeDBlYqzn4VXXwarGKZN7Crd5QfLDT/7KpBM84c9fFUFjFp2wdk6smflRsKKqMa7EgfJJ3Ac2OKlit2pEmBTQfngdpnupoU7BUtRGiiTe7fXiRqmK+KuDn6TpvYogmBRJcrOwIJLIWxmM+dOsyLKryQAaJpjJ1/AxrGO3SqdZt7kKZJrzJWBg5piHENuY8vV6e0UOye1TyftvC5l+gZB8SHJTwpSx4q4JeTUKaxhXoR57h7Rn+3iFolJ3xvPhab6HgJG/pJ7jsNP4sUX+jZiCgEsWd/DjH5IrSYpBUAr0yHpzSoXKOP25a6OBhndh0zcX1qIYM2RIbu6i0KiHD5B/GTMHG03kTGpEL7H80wHFOWwhqDZ+SpkBOtCDYJDhZE4gRcKNbYynAqbCMbXpwpVPFbEng0aKJGbYzK1p4wIegLlcEPmdt+DjXbzcsxFlCynRwwVAwW6hjqeg0Zt521SYCWCJvbe0Un29UDx7Hgrs3IEitHXkw3jOv2fl92D8BBgAJeyqBh90ENQAAAABJRU5ErkJggg==);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-eps {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAmlJREFUeNp0U01vElEUPfMFCEVArdoSqEA0KV246UJdUJM2Lo2JK/9FjXu3utJqTNz4D9worrsQExbFpAFT0TYp0CZ8pIAiyMfMvBnvm2Foa9uX3Lw7c98979x77hNM0wRf7ufPsq7Z2SQYw2QJAkDxQalUZa3WI8hy3gmZr15bu+z8kILBkCeRCJi6bufKMji0NhwiCQR6iitdatTvQ5LyOLLEiWcYukm3m4Zhmbq1BX13FyoxuH7xAlbvpqKRK1fT0PWbRwEmDEyiy1QVg/V1GO02tO1tKLEY2PIy3KEAlmJRDLXb0TeZL+n9g4MHlLJ5HIBuYnSzXq+DlcsQLk/D9Hoh1WrIUjlPcpsYGQzS3LWoaBhvKeXWMQCDA1D9pt8PaXERUjwOjEZQFhZQp9L2yERiqYRCkPt/z58ogTGqHQLE1BLgUmC6XGD5AlipBIFKkbhanKHGYLBDqQ4ZED0OAbfLlo8OIxwGvhVgyTHlA3xkomjH/gegBgDURMv6faDbBZpN+/tHkUApkdTA/PwZAPxntwdUyjYA/+ZMqJHjLgM9iv/6zRt2GgMaIE21aVIjnSm0DGPfmhzyde0UAE2Dj+p7urKCPvkZku9eJILOSMUnkvVhIo7GYIB3xSKYdhoA1erXGVKXpvFxZwdBonnD68PQ7YEwM4O4xwMPxc8RYE87g4FIcz+kvfmnA0YzIJIy77/m0OCqsTkkCTysKPjJG3viLei63Gm3kCO6UWqcMejjxecMPmxsoFKtYop6UNirYL9Wtc5OHqzznIXHq1na7OfMJROcK8a6O7MjW7nfzZdrd7jzT4ABACh3NGsh3GcdAAAAAElFTkSuQmCC);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-exe {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAo1JREFUeNp0k8tPE1EUxr+ZzvRJO62lUAQaKIQ0FVJFjBBdoIkrDDHuXJi4NnHtX+HCjW408Q/QmHTRaCRRohIJifgiiBICTQu29mHfnc7MHc+MlECKdxZz595zf+c737nD6boOYzxJLC6Nhwej7e/24HkO779s7G6mMjcEwfKZ21+/d+em+RbagaFev28qEpZwzKg3ZckqCPH1nfS8hScIdyhBe6JqTG3PfyTTeLrwFhvbKdy9/xi5QglXL0yGJsKDccZY7LDIAwWHpSferWBh+RN8ni4UylVER8MY6PHj0uSpUK0hxzfTmWsUtnoEwO3rer64jEyxim6/Hy67DXaHExvJX3jw7CX8XjfORUdDlOohhU4fAVjILCPbm9V1yIqK2FgYt+ZmsZcv4lH8Nb5upXD7+hVMjIRQa8qeDg8UTYPU5cTcxSk4nS709XTD53ZhpD+IYMAPj+TBz93fZiz5oHV4AP1fGdlyHZIkIZkrI7GyhnK9CZXy+Aig6p1+HQAY003AcF8AVtGGfLWG9XTO4MLZ5cL0WAixoT4zVmPHADSiMo3hzHA/xgeDWFjbNg8H3A7kKnX0koEcPdTu/ylgRGZgOjNv38zoSXC8BZJDRKOlwGEV0VJVGM0y4joAPO1spXbx6sNHeD1uRIYGUCxVSRlDt1fC8rfvcDnsmJ+dOaLgoAs6AVLZPJJ7WdhEkUyT8GJpBflSBcVKDTvpDBw2GzQqQT1OgaZqUOhtFQUTUKnVTVWNpgy51YLVKph7sqKYkA4A1ScEfT66vm5kC3+ofh6Xz59FQ5bpkvE4QW3M5Apoyorhl9ABIKnFgNdTOh2NkJG6WSf9eRBJtmFwLDJmriUzeaOkYvvcXwEGAIVNH6cDA1DkAAAAAElFTkSuQmCC);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-flv {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAmtJREFUeNpsUl1PE0EUPbssLYUCXdpaC9gWoSTgAyFigiRGY+KjvuuTr/4A44MP/gx/gMYfwIsan0RjIjGiJIZgSIGFIoXSD0t3Z3dnd70zpITazuZmJzP3nnvumaMEQQCx3jx69SV3a3KWMxetpSgKxP3m242Do43SQy2k/YRydvds67n8a63k+FRSn7l/bdg5tdsAuM3he/5weDC8vLdqPLgIIpba2niux52mg//DqlsYSg3iztO7mczN3DJ3+ByCLgCBH4hOFEF7cDpzPCRyOpaeLGXSc2PL3HbnW3XaRQCPEgWI2MsRVAVqrwbX9bHxbhOKpiJ/bzpDOr2k68V2BtRNzMtqDEqPejY/4zSGjb54BM0mQ8k4xsDoIMauXxnqYOD7PmwScP31d0SS/eAuh1lrolFpIBQNQw2pqJdqsAlIceB1AJCIkkE/FZskXDQVRXw6IYHiE0nBEcaPXSSvJnGwWkQXAE4acAhbxPMJpOdHweoMhc9b2F8zwKizbdlyPLVH7QLg+JKBYzoorxzjz3oRzUoToaEw9KyO8XQW5AE5jrFT6AbAYVVNxCZ0Ka3So+DSTAoDiej5ywTySbls1OEDobhFlMcXxrHw+AbINEjNXgb7y6BndLhk8cRkHHbD7g4gEhiJFxsdhrDqaamBaDKKerGGSKwPI9kR9EZCaNA5ubE7A5s8IFhsrxQkgJhZoa/06xC5xRz2v+3BOjFlbqcGlquxsondT9vY+2pAJdeZR6fI355CgQCN2A4O1w7gkQ7cdLUOAKdhV6uFSv3kd/n8mT68eC8dKWLnY4FsfeZQh7nVVt0/AQYAsf5g+SvepeQAAAAASUVORK5CYII=);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-gif {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAmVJREFUeNp0U0tPE1EU/trplAqlL0laiw40xASByEJIZFGVnSvj1j+gWxNXJq7VrbrwF7h10cSNhMRHojEuACVBKmH6SJQyJeXRxzzv9dyZPiCtN5lMe8853znf953xcc4hztDzZ1+C6fQMHAfd4/MBFG+p6h/n4OAeAoGNToi/eOm+A50LKRaLh6amoty2vVpZdotNXccMEK3LwZxa2bsDSdrAqePv/mLM5tSdMwYBYqyvw9zdhUn/L59P4OGtG8qlZCoH254/DdCdQBCxqZu+ugqnWoW9swN5ehp2NotgIo6bGQWGtaS8+vQ5V9a0u5S+1gfABEilAqdUgm98HDwUQkDT8JXoPPq+BoM5kCYmFT9jryn1+hkAt7heBx8dhbSwACmTAUwTgdlZ/CVKJaLnI1GD8TikZiPSR8Gxib8chH95mZTxgwWHwH7+gFMswqcokIRbjMO2HDCnZ1VvArpjEmnKZc8+cZJJYGsLsMiZ8AgwEqaY6Mb6RQR33JFhGECzCRyfAFXNu9v+RVNRZWIMuDJNuYMAaDycUFGhCOgtuAtFVDA83G5A8TrFDw+F5QMAxAKJJxz2xnW3RPJGbm+rCyjotZetH4DGzaSSeDA3h4Zl4R0JOEZWTpIzF4n/m995bNdqZwB6m0gFft3Ak6vz+KYWwFsGlqIxXItEcDt1ARMEtKdVgZb+fwA0G2C2hXM0ZTZNRcSf0b1pmXi7uYnjI+Lfanm5fRQsK8BIxKcrK7i/uIgP+Tw+FlREqHN5fx/vyU4uHBE6UO4gDWqk/JFaLuMxcXeFk6TuJ90V0HOk1in7J8AAjmgkPfjU+isAAAAASUVORK5CYII=);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-h {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAbRJREFUeNqMUk1Lw0AQnf0woK0ttVqp0hwqVCl+UBERT94F7x78Cf4Uz179DT14F8WbYHtRkBYRLNqDtdaPZLObuLs1NGlXcWDJZGbey+x7QUEQgIqT07PL5WKhHL5H46J+22q22vsWpbWwdnR4oJ80LNiz2czGUjENhvj4ctIE4Wrj8XmPUlKL9nCYcOFzE9j1OKSTCdjdrtiLdr7KhVgzEvwW6krC92E6k4Kd9bJt57JV5vFK2KfRQRV+RAMkzxglYI1RaDy2dW1rpWRjQo5VGicYIorWVooFvQVCCAjG8Omw1MgG8AM0uSBUDSnCfk/IGCHwf3DCD/7UhOLBrFkDuep/hDUSSCv1iYo4rIfqGwmUSNJjfYbBcQKhZw0aBMA4B48LwBhBt/cON80HmM9NQ6fXg/Wlku4TwmNWDzaQqzHG+0PSKod5cH5Vh2RiAhYKc8DlV1UPSyuFMGygVlMg1/P6BC6DqXQK8jNZDXAYA1f21V34wMXYFaiyVw0rJyzLgs3VMkxOjGtix/V0XWChZ0cI2i/dzvXdfTd0Qf91BMPrhyNzgKfOmxaWypqaDXHfAgwAtCL8XOfF47gAAAAASUVORK5CYII=);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-hpp {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAehJREFUeNqEUk1v00AUHK/XKf1yZdESVRBXjRSRFqMQVBA5Ic5I3DnwE/gpnLnyG3LgXglx4UDDLZS0RWkDLiRxSusk9u6GXSembmLgWZbX7+2bnZl92mg0goo3b3ffO/ncdvyfjHef6q2Dlvs8Q2ktzr16+SL60jhhZ69bO8X8ClLC7w9XdKJVG8fuM0r1WrJG4gXjgqU1D0MGc2kBTytl+7a9XmWcl1IB/hZKEhccq5aJJ/e3bTu7Wg1CVo7rNLlRhUh4oMnXoDoyhoHGyWmUe+QUbELIa7W8CjAFlMzdzeckCwFN06ATAn8QmDMMMGlMuwWucpoCHNe4jBkAMenjYvRPTyi53JvuwX8AplleAeBcRFrH6rXIxLim9I/pi3QA1RhKaYxdjkN8IwalCMIwWs9ljMkh0wzk+9M7w179C3LZNXxve2h+c3Hu91HeKmD/6zHOLnw83ilB1/V0CeqU3Q81LC/O41b2Btx2N2JVP2riR8eTUxmi0TzBwrKZMsqMoz8MsDh/DWuWhUBKURLKxQIeOMWoptYPnS1c+INZBkwISomOSsmBZS7B+3WOzZvrKGzkMAiGqNy7g+LmRkRfekBnANy2163PZXrSbrQ6vch19Xz8fPDHyL39QzkHBKedXjfu+y3AAGU37INBJto1AAAAAElFTkSuQmCC);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-html {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAmBJREFUeNqEUktPE1EU/mY605a+hhZTBNKRDApNrWIRA4nEBUZdmCgLNi4MK5f+FNdu3bFv1J1EXODCR1JJSMTwpqUP6NiCpe10Zjz3hj5Mm3iSybl37jnf+c53jmDbNpi9eb+6Ftcisea909bWNzNb6dwzSXKkhIt/r14+515qBqmDA8HpqKagh53XaopblpIbe+knDpFAhPab2Dw0TKvRK7lmNODzePBgZlK9oUWSpmVNdpIU8T+jaMsyMaD4MDcZVa+NhJMN00w0n6V2nN3yQgdHWZag+LzYPTomIAtT0THVtPGanmb/BbjwLFkvn2IttYGYplKyDzsHh7gdmyAWfh5zVq0Guhg4RAHFUhmfvq3j134aXo8bd+ITnMFOOovU5jbGRoZwNxFn1cxuAIcDW/sZDjA/c4u+BNxOJyxqaenpI3z88gMfPn9Hv98HQZS6RazW6kjExvFi8TGdDSy/W0Emf4LS6R8sv11BmfzSwkPcm74Jo9Ei0GZgmkw8QCOao8OXcaz/5vSZnPdnp3ApqBBLkWJE0Ci7ASzbIhCLLQ1E0iOkBDh9NpUgiUejo8oNuJwyn0YPABtn51UYFFivG3yBGCNZkuDtc/MW+ZQI3OrYpBaARCKufk3B5XIiWyhiL5ODp8+FfFHH+KiKSqWKUL8fC/NznGlPBmz+24dZjKnD0CJDcMoyW0SqXuMtHBFw7rhIAD1ErNUNafxKBNevapwu65NpEQ4FqXIA+RMd6VwBP3cPSERb6gLIFIq61+UqGWaFdcrVt/lmAuWjAi2aiMFwmOYuIJ/N6M28vwIMAMoNDyg4rcU9AAAAAElFTkSuQmCC);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-ics {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAhRJREFUeNqEUkFPE0EU/mZ2dra7bLNpi2AxQFKalkJrohICiYkXPagXrx78Df4K48GDBzmQePLMhUODNxQ5ciEkJVqDtJGmMWrCATRbd2ecoS5u3aovmezsvu9973vfPiKlhI4XL7c2r5YL81LIELEghLA3u/udxmHnPmfGW/Wuv+LpwwdneRYBx7PeWK0wOYYhcXxyckGV1fdbnbuMsXcklqPRJQxFMKz4RxDCtVO4s3xlRjWoB0FYjlQPEEBieChwKCRGMx5uLtaKs1P5ei8IKlGa/YkXMXYtlTEDlsnw/mMXhBJcqxSK6vlcpa4PEpCooUyIqs5M6hG1o2CUwqA091cFcYLf/sjzcX75EiQIojI9779CTYR4jwTBf+r7GAwh0AxCiL6JMT/04vQ79u8aI2O/7Jzg69o6Go8ewycUahtBpADhHKLnK/eVbkMdtROWIv80NQ2sPhncA9Htwn+9hZG0rY6DzFwJl+7dhs0ZstUy8rduwPS/wd/ehmi3kwq4zTHiWUgXp+EuL8FvNvFl5Rn4xAS86iyI2kY3n0Mv48ByrOQmancdi8I0Kcj3U5iuA29xAelKCUHrEIayzltagG2E4IwkFaQgSC6lYI09iN0d8It5uNV5nG5sgJdKYC0G8WoTOZvBISFNEBxnsuzD3GX4vfDsszzqAu0jkJQDedCGbB6AWg54pYbPo+NGVPdTgAEAqQq70PytIL0AAAAASUVORK5CYII=);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-iso {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAjlJREFUeNp0kstrU0EUxr/k5qbJzdPYpGkpsUJoA2q1oLjTdiGiIC5cuXHlxv9BEOrStTvBnQvRrSAIsejCrlqpsURq2hCJNQ+TNLm5uc/x3MmzJh34mDNnvvnNzOE4GGOwx8+t9XQkfn0VE0Y5/7Z+kHm+dvOhtd3P9c/xwNZh7nWaMYtNUmX/Fct/vlN7/8J5aRRgyzm8xzpRDjGE2aVH4VTqdnoUYg/XkEhmy+Cx3DhA5tMzdFolvg5Mx3Fx9SmH0JIg79Zo3j4GADMIokJTKtjbfAKXU4Y/2NvSfyH75TFOxa9Cmr0XnlPFl5ReOQ6wNMDsoFX6AElqQlNV1KsOuNwS/AGFjEUIDhmn5+/DMM16/9igBowAzFKIswPJr6MjlxFP3sV04gaP7RzMPe6xvWM1gNUBM2UKYlBau3QghGphg29J3gDlLLilWNdD3gkvIIDRhD9yGe2mCV0V4HFXuCxT5Dlv8Dz3sIkAs03FalDxBMQSt9BRBMhNncuO7dyU28c9tnf8C/Q0ZtR4GImeQSj8APLRH772BWcgiFODffCv/t8H9tO0v3RjV7VqkeeXLlzDfvYjj88uXhl4JwIsrYxmLY/M1gYclIvGE9jZfNPrSCD3/QgLyeWTADV6wW9AryIcCkB0u1Aq/oCPumlufoF72vIheaLDr4wCLIOqrYnULA14PSoqpSJEAUilZrD77Sv3LK+cI0+Be8cAbbmAOrob0agtD491LYfkoqvnyZLsWRkA/gkwABL4S3L78XYyAAAAAElFTkSuQmCC);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-java {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAjxJREFUeNp8U01v00AUnNiOEyepQyhQobRBSlVIoRCBEPTAjQsSEneE+An8FM5cuXLNoQduIAE3qopKNJAIIppA2jrOR93aa6/N8yZuUxyxkrXr3ffmzczbTQRBgHC83nj3ca28dD36nx6fvnzrNNrdp4oibyUmey9fPBezEgWVFuYLdyvlPGaMY4fl1aRS+9pqP5ElAkmcnknRwuO+Nyt5u/ETYfyj9WrpZnmpxn2/Ok1Swn/GvtnH5k4TLue4kNfxoFoprRQv1TzOb8cAIu3+ZD7oD/Hm7XuxzqRUNDtdkuLiTmW5tFxceBXlnXgQTAORSMt2oGezUJJJrK9dFWdEH7Ik4dB29LiESeUEJXd7/dAT3L+1ivlCHr8NEzutXTBvbJPPSdO/AH5wysChwM/1HzCGlmAzOrKxu2eCud6Z2Jke2MwThpUXL6Nn2ZAVFTlNw70bK0iRnGAq9qwHtOmTRpsx1NsHyKRVnNPnoMoK9kc2BjbD4vk5JGV5NkBoEPM4FFnCteJFWOS4ntHEfphQyKaFTWFLw704AJ26ZFx/ZEEi3YyY0O1Dmr4EKTUHA8hUnS6siI0DEHLYog+b28RCRuNXR/iQUpPUEQ+NVht6Lodnjx+GXYgDSFRnq97Ed2pXSlXhUSeGhxYc5sKlNXM5DGLR2TMwfZVPAIi+otGNWy1fEZUKeo4qc4ysI+F8VksLIJfYcD9QYgB/DNPMptWBlsnBIS86xmDMTBo/PWd0LB6VZfdEbJT3V4ABAA5HIzlv9dtdAAAAAElFTkSuQmCC);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-jpeg,
.icon-jpg {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAmlJREFUeNpsU8luUlEY/s4dmMpkWxRopGJNNbiwhk1tItbGtXHr0hcwmvgOdWld6Bu4coXumtREE3ZKu8FgOlC1kIoXtC3jPfdc/8PUIpzkBM7wf+f/hsts24YczuerGUc0moBlYTAYA+i8sbdXtAzjITRtq39kr73s/Gr9DTUYPOeamwvYnHdrdR0SnDebuCbswJGqpX+Uf92Hqm7hzFAG/4TgNr1uCwEJ0trcBC8U0Kb1/PQkHt9JxSLnL6TB+Y2zAIMOJBGLXmtsbEAYBsx8HnqCGKVScAX8uHf5EpqmGXv18VO6VDEe0PXsKABN8+AAgiabmYFNNJTDQ2RUFc8+Z9G0OPR4PKYwvKari0MAgiY/OQGCAajhMNR4nDZMaInrKBGl70SPMScck1NQG3X/CAWLE3/dAWV5hRRVIJxOWNksrP19sFgMqqAebUGYHMI6teq0A9oTVAhqu2sfbYYjsL7lCZ3683gA70T3TK7/B4BNoO020GwB9TpwfAz8LgMtWn/NkV8EHgoB81c7nYwCyBZlEVkHcqMTKFnkmehJTOPvEfCnKi0fAyADJKfXC/h83TaZTJjaa5lANLpOFqAXtlEAorAwO9u5syT5UxLfU0e3o1FMu1x4u7ODYq02BKAMAVSrSNLrK1MhLPj8mNF0vFm+C1ZvwKBwXXE4AGn1WAASazESwUW3BzUSMeJ2o1Aq4sPurvQYSRLwlhRR6mSaYyi0WlpAJrFRx3ouh5/lMt5lv8BLwXp0M4lSpYL17e2uK5wP6lj/c2ZPn2RI+YT8fDvqoyegVLyfG5kBKaQQOfvF2pLc+ifAABiQH3PEc1i/AAAAAElFTkSuQmCC);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-js {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAyJpVFh0WE1MOmNvbS5hZG9iZS54bXAAAAAAADw/eHBhY2tldCBiZWdpbj0i77u/IiBpZD0iVzVNME1wQ2VoaUh6cmVTek5UY3prYzlkIj8+IDx4OnhtcG1ldGEgeG1sbnM6eD0iYWRvYmU6bnM6bWV0YS8iIHg6eG1wdGs9IkFkb2JlIFhNUCBDb3JlIDUuMy1jMDExIDY2LjE0NTY2MSwgMjAxMi8wMi8wNi0xNDo1NjoyNyAgICAgICAgIj4gPHJkZjpSREYgeG1sbnM6cmRmPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5LzAyLzIyLXJkZi1zeW50YXgtbnMjIj4gPHJkZjpEZXNjcmlwdGlvbiByZGY6YWJvdXQ9IiIgeG1sbnM6eG1wPSJodHRwOi8vbnMuYWRvYmUuY29tL3hhcC8xLjAvIiB4bWxuczp4bXBNTT0iaHR0cDovL25zLmFkb2JlLmNvbS94YXAvMS4wL21tLyIgeG1sbnM6c3RSZWY9Imh0dHA6Ly9ucy5hZG9iZS5jb20veGFwLzEuMC9zVHlwZS9SZXNvdXJjZVJlZiMiIHhtcDpDcmVhdG9yVG9vbD0iQWRvYmUgUGhvdG9zaG9wIENTNiAoV2luZG93cykiIHhtcE1NOkluc3RhbmNlSUQ9InhtcC5paWQ6RUQ5ODY5Q0NGMTE4MTFFMTlDRjlDN0VBQTY3QTk0MTEiIHhtcE1NOkRvY3VtZW50SUQ9InhtcC5kaWQ6RUQ5ODY5Q0RGMTE4MTFFMTlDRjlDN0VBQTY3QTk0MTEiPiA8eG1wTU06RGVyaXZlZEZyb20gc3RSZWY6aW5zdGFuY2VJRD0ieG1wLmlpZDpFRDk4NjlDQUYxMTgxMUUxOUNGOUM3RUFBNjdBOTQxMSIgc3RSZWY6ZG9jdW1lbnRJRD0ieG1wLmRpZDpFRDk4NjlDQkYxMTgxMUUxOUNGOUM3RUFBNjdBOTQxMSIvPiA8L3JkZjpEZXNjcmlwdGlvbj4gPC9yZGY6UkRGPiA8L3g6eG1wbWV0YT4gPD94cGFja2V0IGVuZD0iciI/PoT8zQ8AAAJdSURBVHjadFNbTxNREP52t7S0bktbKFAvTUVaw60YqkExUTD6oD74qC/yD/wp/gh885XEEI0RAyYQUiMpIBGMkYR6o23abi+73e2uc04v1LROMtnZPTPffvPNHMGyLDB7sbJ2ciUSli3U35smkK9t7x9v7n2dD/g8KUkUwWqeP3vKz23NxJGzgwOx0RC6mSgIo+WKuvP56MeUzy2nJEk8PWsGJVVTuhWbpgmHw47FB7d98Wg4mVWK52o1sxOg3Va3PmFp+Q2PdUquaFUM9/vw+O6cP3bxwm46Xwh1ALR3/vL1e+hGjcc9koScUsTSq3coVDQsXJ3wzo5HEs3clgZNMTVdx1T0Ep7cn6//QRQwMhzA6uZHLD5cIFEFSKIU+G8LK+tb0KsGZKcTJoEyP08AbpcLy6sbPKdQrigdAGaDwWxsDH1uGbliCYIgcM8WFPg8Mq5Pjzdyu4jYbCE44EepXMHuwXe+A8x3KKYxYsjvbUzmlPGpBmYdgI1oYjSMbL4Ao1YXMkcM2Dd2xnbAamPQAqg1GORLZdycmYTdJqFKk2DPR3fmwI4zBDrg9RADqxPAbPBif2WTSB584/3/TGegEOit+DRcvQ4OZJi1LgwIQKVCg2i6nb1I7H3Br3QWqT9pBAP9uDY5xjdSM3RqxeoUkfVnEOW8UkLykERTNXjkM7h3Iw6NNvHw6JjuhAhVrba0+QeALozcI9nQR0VvNxJc/ZmxCNGvIBQcpDG6udA22kyW29HC72wu8yG579ZoiSYuR/ly2+y9CA4NceWLmo717T1i5ULqJNtapL8CDACskxPFZRxLwQAAAABJRU5ErkJggg==);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-key {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAlZJREFUeNpsU11PE0EUPbM7u/2AtJUWU6qiiSYYo5EmmPDCD9AH46sx8cEnja/+CB989z+Y+MKPgMiDsYQACcbaWBBogYD92t2Zud7ZlQZsbzKZ3bl3zj3n3IwgItjYeDO3MlWme0bjUth8e8/fO2tHzx3XqUEk50uft+Ndnhdmc3SlfNPkVZT8Cy600DoIISvVfKYtlvfX1p66XmoIYsMZdjJQWvEFbbsC/S5g2QhSkKUK7rx6OzvzqLpsovAhaAxA3DUBQn2TUFsl7KwTfm4Z9DoO5LW7uPXi9Wxpfn7ZKF09vyPxX2iWcNRkKGZz0mQWKoNs8AVB6x1yRY2pYnc2LLofuXTxMgAlmlXIfngCxNxEzM+DPv6NQa2BygLgZyX6JT83ngHTN5GAL0WSoUQkSQnXkyBh/k0GegTAaldM20sTKvet+yyhIZApECamL0jUSe3oFChx3TopM4TeEQP2gc6BgGIwb4KGNXRhCkMGxgg2kJeybRiZM45D8W61qEAknSmpHStBhywu0nFVupSCTAcM4ECwqapv+NQ6LS9JGALoMIIoPYDjZiEL1xHtbyO39AQUDaA7R1AH23DSeSA4hv5RG/VAhxomPYP8sw9A4TaC9iHkjUWmrtGvbyC18BLe3GP0m3WW4I5hEBEnPIStXzyuFIxb4EkMEJ79Qa/xHbKxCdM7xeCwzUZOjgEwnuzt7qLz6T3cySmQP43uzjeIiTJM6io6W19B/NLCKMVGCzkCoLR/0lrfOI2fNy/huKC1FTsK/rbGNeMRC8dHpHByfu+vAAMAL/0jvAVZQl0AAAAASUVORK5CYII=);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-less {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAyJpVFh0WE1MOmNvbS5hZG9iZS54bXAAAAAAADw/eHBhY2tldCBiZWdpbj0i77u/IiBpZD0iVzVNME1wQ2VoaUh6cmVTek5UY3prYzlkIj8+IDx4OnhtcG1ldGEgeG1sbnM6eD0iYWRvYmU6bnM6bWV0YS8iIHg6eG1wdGs9IkFkb2JlIFhNUCBDb3JlIDUuMy1jMDExIDY2LjE0NTY2MSwgMjAxMi8wMi8wNi0xNDo1NjoyNyAgICAgICAgIj4gPHJkZjpSREYgeG1sbnM6cmRmPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5LzAyLzIyLXJkZi1zeW50YXgtbnMjIj4gPHJkZjpEZXNjcmlwdGlvbiByZGY6YWJvdXQ9IiIgeG1sbnM6eG1wPSJodHRwOi8vbnMuYWRvYmUuY29tL3hhcC8xLjAvIiB4bWxuczp4bXBNTT0iaHR0cDovL25zLmFkb2JlLmNvbS94YXAvMS4wL21tLyIgeG1sbnM6c3RSZWY9Imh0dHA6Ly9ucy5hZG9iZS5jb20veGFwLzEuMC9zVHlwZS9SZXNvdXJjZVJlZiMiIHhtcDpDcmVhdG9yVG9vbD0iQWRvYmUgUGhvdG9zaG9wIENTNiAoV2luZG93cykiIHhtcE1NOkluc3RhbmNlSUQ9InhtcC5paWQ6RjZERjZENTJGMTE4MTFFMUIwOEVERjQ5MTZEMkVBREUiIHhtcE1NOkRvY3VtZW50SUQ9InhtcC5kaWQ6RjZERjZENTNGMTE4MTFFMUIwOEVERjQ5MTZEMkVBREUiPiA8eG1wTU06RGVyaXZlZEZyb20gc3RSZWY6aW5zdGFuY2VJRD0ieG1wLmlpZDpGNkRGNkQ1MEYxMTgxMUUxQjA4RURGNDkxNkQyRUFERSIgc3RSZWY6ZG9jdW1lbnRJRD0ieG1wLmRpZDpGNkRGNkQ1MUYxMTgxMUUxQjA4RURGNDkxNkQyRUFERSIvPiA8L3JkZjpEZXNjcmlwdGlvbj4gPC9yZGY6UkRGPiA8L3g6eG1wbWV0YT4gPD94cGFja2V0IGVuZD0iciI/Pl1w97IAAAJhSURBVHjahJNLbxJRFMf/wPAIMIxMkUI7tS0VYqlGDLGhjdKkqyZ24cJFN925de+XcONHaHRj4k7TND6SGo1VWwmp2kSLhlqMDbQ87gzPYcY7k4GgoJ6bmdw598zvnvM/95pUVYVma+svcovx8yMnFZHAMJPJBJfDzq5vpX6+/vD5qo/z7DOMBdo/d26t6jFMJ3iY51jBz4M+LP6wxEw40Gy23qYzB3HO7fpmpZCOmfEfa7Xb4NxOrC4lvbPToe2yKE3K1PdPwNOtHdx79ESfq4qKkijB5/XgevIyHxEC24USmewDqD2ABxubaLRkfW6zMqjWGlh7/ByyAtxYnOPnL0Q2+gGGmKRaw8zUBJaTiS5QOO1FJnuIAM8hciaIWHgi8NcSNt+loVDY8JBXh2ojJAR1HbTSNFMUpV8Dxcjg0nSYBrtBxdLbqI1iheCUh9XXNGurAwCdEkb9QyBSFam9TDfoPZ1LUg1BH28IiwEARTVAQOzcFKRaHZpLoa9avY6L1Gfs0c32t4PU6W2lWsV8LAorw0Cs1nXftYWE3qZGqwWHzYp2zzlgetuolVFvtiDLbRRKFTAWCxx2G/KlMtXFhWPqOzsWHJwBx7rxKv2R7mwFz3lw9/5DLC/M4Us2RwV0g3U58XJnF7dvrsBOoX0Abbej/DFKRMKI30fTVGC32WA2m5H9cQQvhYi0vE/7Wdgczn6ARA9QPBrBszcp/XvpyqxebzQ0Tlsq6llxLhe9bD4cFMr9XdjLHpLv+SLGBYHAYiVu1kNOpAaRTWbCejgiw0zGhFGSK1aw+zXbvfK/BBgAPwADAs5GpGsAAAAASUVORK5CYII=);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-logo {
	background-image:url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 553 235.3'%3E%3Cdefs%3E%3C/defs%3E%3Cpath fill='%23ffffff' d='M239 63h17.8v105H239V63zm35.6 0h36.3c7.9 0 14.5.9 19.6 2.6s9.2 4.1 12.1 7.1a24.45 24.45 0 0 1 6.2 10.2 40.75 40.75 0 0 1 1.8 12.1 45.69 45.69 0 0 1-1.8 12.9 26.58 26.58 0 0 1-6.2 10.8 30.59 30.59 0 0 1-12.1 7.3c-5.1 1.8-11.5 2.7-19.3 2.7h-19.1V168h-17.5V63zm36.2 51a38.37 38.37 0 0 0 11.1-1.3 16.3 16.3 0 0 0 6.8-3.7 13.34 13.34 0 0 0 3.5-5.8 29.75 29.75 0 0 0 1-7.6 25.68 25.68 0 0 0-1-7.7 12 12 0 0 0-3.6-5.5 17.15 17.15 0 0 0-6.9-3.4 41.58 41.58 0 0 0-10.9-1.2h-18.5V114h18.5zm119.9-51v15.3h-49.2V108h46.3v15.4h-46.3V168h-17.8V63h67zm26.2 72.9c.8 6.9 3.3 11.9 7.4 15s10.4 4.7 18.6 4.7a32.61 32.61 0 0 0 10.1-1.3 20.52 20.52 0 0 0 6.6-3.5 12 12 0 0 0 3.5-5.2 19.08 19.08 0 0 0 1-6.4 16.14 16.14 0 0 0-.7-4.9 12.87 12.87 0 0 0-2.6-4.5 16.59 16.59 0 0 0-5.1-3.6 35 35 0 0 0-8.2-2.4l-13.4-2.5a89.76 89.76 0 0 1-14.1-3.7 33.51 33.51 0 0 1-10.4-5.8 22.28 22.28 0 0 1-6.3-8.8 34.1 34.1 0 0 1-2.1-12.7 26 26 0 0 1 11.3-22.4 36.35 36.35 0 0 1 12.6-5.6 65.89 65.89 0 0 1 15.8-1.8c7.2 0 13.3.8 18.2 2.5a34.46 34.46 0 0 1 11.9 6.5 28.21 28.21 0 0 1 6.9 9.3 42.1 42.1 0 0 1 3.2 11l-16.8 2.6c-1.4-5.9-3.7-10.2-7.1-13.1s-8.7-4.3-16.1-4.3a43.9 43.9 0 0 0-10.5 1.1 19.47 19.47 0 0 0-6.8 3.1 11.63 11.63 0 0 0-3.7 4.6 14.08 14.08 0 0 0-1.1 5.4c0 4.6 1.2 8 3.7 10.3s6.9 4 13.2 5.3l14.5 2.8c11.1 2.1 19.2 5.6 24.4 10.5s7.8 12.1 7.8 21.4a31.37 31.37 0 0 1-2.4 12.3 25.27 25.27 0 0 1-7.4 9.8 36.58 36.58 0 0 1-12.4 6.6 56 56 0 0 1-17.3 2.4c-13.4 0-24-2.8-31.6-8.5s-11.9-14.4-12.6-26.2h18z'/%3E%3Cpath fill='%23469ea2' d='M30.3 164l84 48.5 84-48.5V67l-84-48.5-84 48.5v97z'/%3E%3Cpath fill='%236acad1' d='M105.7 30.1l-61 35.2a18.19 18.19 0 0 1 0 3.3l60.9 35.2a14.55 14.55 0 0 1 17.3 0l60.9-35.2a18.19 18.19 0 0 1 0-3.3L123 30.1a14.55 14.55 0 0 1-17.3 0zm84 48.2l-61 35.6a14.73 14.73 0 0 1-8.6 15l.1 70a15.57 15.57 0 0 1 2.8 1.6l60.9-35.2a14.73 14.73 0 0 1 8.6-15V79.9a20 20 0 0 1-2.8-1.6zm-150.8.4a15.57 15.57 0 0 1-2.8 1.6v70.4a14.38 14.38 0 0 1 8.6 15l60.9 35.2a15.57 15.57 0 0 1 2.8-1.6v-70.4a14.38 14.38 0 0 1-8.6-15L38.9 78.7z'/%3E%3Cpath fill='%23469ea2' d='M114.3 29l75.1 43.4v86.7l-75.1 43.4-75.1-43.4V72.3L114.3 29m0-10.3l-84 48.5v97l84 48.5 84-48.5v-97l-84-48.5z'/%3E%3Cpath fill='%23469ea2' d='M114.9 132h-1.2A15.66 15.66 0 0 1 98 116.3v-1.2a15.66 15.66 0 0 1 15.7-15.7h1.2a15.66 15.66 0 0 1 15.7 15.7v1.2a15.66 15.66 0 0 1-15.7 15.7zm0 64.5h-1.2a15.65 15.65 0 0 0-13.7 8l14.3 8.2 14.3-8.2a15.65 15.65 0 0 0-13.7-8zm83.5-48.5h-.6a15.66 15.66 0 0 0-15.7 15.7v1.2a15.13 15.13 0 0 0 2 7.6l14.3-8.3V148zm-14.3-89a15.4 15.4 0 0 0-2 7.6v1.2a15.66 15.66 0 0 0 15.7 15.7h.6V67.2L184.1 59zm-69.8-40.3L100 26.9a15.73 15.73 0 0 0 13.7 8.1h1.2a15.65 15.65 0 0 0 13.7-8l-14.3-8.3zM44.6 58.9l-14.3 8.3v16.3h.6a15.66 15.66 0 0 0 15.7-15.7v-1.2a16.63 16.63 0 0 0-2-7.7zM30.9 148h-.6v16.2l14.3 8.3a15.4 15.4 0 0 0 2-7.6v-1.2A15.66 15.66 0 0 0 30.9 148z'/%3E%3Cpath fill='%23083b54' fill-opacity='0.15' d='M114.3 213.2v-97.1l-84-48.5v97.1z'/%3E%3Cpath fill='%23083b54' fill-opacity='0.05' d='M198.4 163.8v-97l-84 48.5v97.1z'/%3E%3C/svg%3E%0A");
	background-repeat:no-repeat;
	background-size:contain
}

.icon-mid {
	background-repeat:no-repeat;
	background-size:contain
}

.icon-mkv {
	background-image:url("data:image/svg+xml;charset=utf8,%3Csvg id='Layer_2' xmlns='http://www.w3.org/2000/svg' viewBox='0 0 72 100'%3E%3Cstyle/%3E%3ClinearGradient id='SVGID_1_' gradientUnits='userSpaceOnUse' x1='36.2' y1='101' x2='36.2' y2='3.005' gradientTransform='matrix(1 0 0 -1 0 102)'%3E%3Cstop offset='0' stop-color='%23e2cde4'/%3E%3Cstop offset='.17' stop-color='%23e0cae2'/%3E%3Cstop offset='.313' stop-color='%23dbc0dd'/%3E%3Cstop offset='.447' stop-color='%23d2b1d4'/%3E%3Cstop offset='.575' stop-color='%23c79dc7'/%3E%3Cstop offset='.698' stop-color='%23ba84b9'/%3E%3Cstop offset='.819' stop-color='%23ab68a9'/%3E%3Cstop offset='.934' stop-color='%239c4598'/%3E%3Cstop offset='1' stop-color='%23932a8e'/%3E%3C/linearGradient%3E%3Cpath d='M45.2 1l27 26.7V99H.2V1h45z' fill='url(%23SVGID_1_)'/%3E%3Cpath d='M45.2 1l27 26.7V99H.2V1h45z' fill-opacity='0' stroke='%23882383' stroke-width='2'/%3E%3Cpath d='M7.5 91.1V71.2h6.1l3.6 13.5 3.6-13.5h6.1V91h-3.8V75.4l-4 15.6h-3.9l-4-15.6V91H7.5zm23.5 0V71.2h4V80l8.2-8.8h5.4L41.1 79l8 12.1h-5.2l-5.5-9.3-3.4 3.3v6h-4zm25.2 0L49 71.3h4.4L58.5 86l4.9-14.7h4.3l-7.2 19.8h-4.3z' fill='%23fff'/%3E%3ClinearGradient id='SVGID_2_' gradientUnits='userSpaceOnUse' x1='18.2' y1='50.023' x2='18.2' y2='50.023' gradientTransform='matrix(1 0 0 -1 0 102)'%3E%3Cstop offset='.005' stop-color='%23963491'/%3E%3Cstop offset='1' stop-color='%2370136b'/%3E%3C/linearGradient%3E%3ClinearGradient id='SVGID_3_' gradientUnits='userSpaceOnUse' x1='11.511' y1='51.716' x2='65.211' y2='51.716' gradientTransform='matrix(1 0 0 -1 0 102)'%3E%3Cstop offset='.005' stop-color='%23963491'/%3E%3Cstop offset='1' stop-color='%2370136b'/%3E%3C/linearGradient%3E%3Cpath d='M64.3 55.5c-1.7-.2-3.4-.3-5.1-.3-7.3-.1-13.3 1.6-18.8 3.7S29.6 63.6 23.3 64c-3.4.2-7.3-.6-8.5-2.4-.8-1.3-.8-3.5-1-5.7-.6-5.7-1.6-11.7-2.4-17.3.8-.9 2.1-1.3 3.4-1.7.4 1.1.2 2.7.6 3.8 7.1.7 13.6-.4 20-1.5 6.3-1.1 12.4-2.2 19.4-2.6 3.4-.2 6.9-.2 10.3 0m-9.9 15.3c.5-.2 1.1-.3 1.9-.2.2-3.7.3-7.3.3-11.2-6.2.2-11.9.9-17 2.2.2 4 .4 7.8.3 12 4-1.1 7.7-2.5 12.6-2.7m2-12.1h1.1c.4-.4.2-1.2.2-1.9-1.5-.6-1.8 1-1.3 1.9zm3.9-.2h1.5V38h-1.3c0 .7-.4.9-.2 1.7zm4 0c.5-.1.8 0 1.1.2.4-.3.2-1.2.2-1.9h-1.3v1.7zm-11.5.3h.9c.4-.3.2-1.2.2-1.9-1.4-.4-1.6 1.2-1.1 1.9zm-4 .4c.7.2.8-.3 1.5-.2v-1.7c-1.5-.4-1.7.6-1.5 1.9zm-3.6-1.1c0 .6-.1 1.4.2 1.7.5.1.5-.4 1.1-.2-.2-.6.5-2-.4-1.9-.1.4-.8.1-.9.4zm-31.5.8c.4-.1 1.1.6 1.3 0-.5 0-.1-.8-.2-1.1-.7.2-1.3.3-1.1 1.1zm28.3-.4c-.3.3.2 1.1 0 1.9.6.2.6-.3 1.1-.2-.2-.6.5-2-.4-1.9-.1.3-.4.2-.7.2zm-3.5 2.8c.5-.1.9-.2 1.3-.4.2-.8-.4-.9-.2-1.7h-.9c-.3.3-.1 1.3-.2 2.1zm26.9-1.8c-2.1-.1-3.3-.2-5.5-.2-.5 3.4 0 7.8-.5 11.2 2.4 0 3.6.1 5.8.3M33.4 41.6c.5.2.1 1.2.2 1.7.5-.1 1.1-.2 1.5-.4.6-1.9-.9-2.4-1.7-1.3zm-4.7.6v1.9c.9.2 1.2-.2 1.9-.2-.1-.7.2-1.7-.2-2.1-.5.2-1.3.1-1.7.4zm-5.3.6c.3.5 0 1.6.4 2.1.7.1.8-.4 1.5-.2-.1-.7-.3-1.2-.2-2.1-.8-.2-.9.3-1.7.2zm-7.5 2H17c.2-.9-.4-1.2-.2-2.1-.4.1-1.2-.3-1.3.2.6.2-.1 1.7.4 1.9zm3.4 1c.1 4.1.9 9.3 1.4 13.7 8 .1 13.1-2.7 19.2-4.5-.5-3.9.1-8.7-.7-12.2-6.2 1.6-12.1 3.2-19.9 3zm.5-.8h1.1c.4-.5-.2-1.2 0-2.1h-1.5c.1.7.1 1.6.4 2.1zm-5.4 7.8c.2 0 .3.2.4.4-.4-.7-.7.5-.2.6.1-.2 0-.4.2-.4.3.5-.8.7-.2.8.7-.5 1.3-1.2 2.4-1.5-.1 1.5.4 2.4.4 3.8-.7.5-1.7.7-1.9 1.7 1.2.7 2.5 1.2 4.2 1.3-.7-4.9-1.1-8.8-1.6-13.7-2.2.3-4-.8-5.1-.9.9.8.6 2.5.8 3.6 0-.2 0-.4.2-.4-.1.7.1 1.7-.2 2.1.7.3.5-.2.4.9m44.6 3.2h1.1c.3-.3.2-1.1.2-1.7h-1.3v1.7zm-4-1.4v1.3c.4.4.7-.2 1.5 0v-1.5c-.6 0-1.2 0-1.5.2zm7.6 1.4h1.3v-1.5h-1.3c.1.5 0 1 0 1.5zm-11-1v1.3h1.1c.3-.3.4-1.7-.2-1.7-.1.4-.8.1-.9.4zm-3.6.4c.1.6-.3 1.7.4 1.7 0-.3.5-.2.9-.2-.2-.5.4-1.8-.4-1.7-.1.3-.6.2-.9.2zm-3.4 1v1.5c.7.2.6-.4 1.3-.2-.2-.5.4-1.8-.4-1.7-.1.3-.8.2-.9.4zM15 57c.7-.5 1.3-1.7.2-2.3-.7.4-.8 1.6-.2 2.3zm26.1-1.3c-.1.7.4.8.2 1.5.9 0 1.2-.6 1.1-1.7-.4-.5-.8.1-1.3.2zm-3 2.7c1 0 1.2-.8 1.1-1.9h-.9c-.3.4-.1 1.3-.2 1.9zm-3.6-.4v1.7c.6-.1 1.3-.2 1.5-.8-.6 0 .3-1.6-.6-1.3 0 .4-.7.1-.9.4zM16 60.8c-.4-.7-.2-2-1.3-1.9.2.7.2 2.7 1.3 1.9zm13.8-.9c.5 0 .1.9.2 1.3.8.1 1.2-.2 1.7-.4v-1.7c-.9-.1-1.6.1-1.9.8zm-4.7.6c0 .8-.1 1.7.4 1.9 0-.5.8-.1 1.1-.2.3-.3-.2-1.1 0-1.9-.7-.2-1 .1-1.5.2zM19 62.3v-1.7c-.5 0-.6-.4-1.3-.2-.1 1.1 0 2.1 1.3 1.9zm2.5.2h1.3c.2-.9-.3-1.1-.2-1.9h-1.3c-.1.9.2 1.2.2 1.9z' fill='url(%23SVGID_3_)'/%3E%3ClinearGradient id='SVGID_4_' gradientUnits='userSpaceOnUse' x1='45.269' y1='74.206' x2='58.769' y2='87.706' gradientTransform='matrix(1 0 0 -1 0 102)'%3E%3Cstop offset='0' stop-color='%23f9eff6'/%3E%3Cstop offset='.378' stop-color='%23f8edf5'/%3E%3Cstop offset='.515' stop-color='%23f3e6f1'/%3E%3Cstop offset='.612' stop-color='%23ecdbeb'/%3E%3Cstop offset='.69' stop-color='%23e3cce2'/%3E%3Cstop offset='.757' stop-color='%23d7b8d7'/%3E%3Cstop offset='.817' stop-color='%23caa1c9'/%3E%3Cstop offset='.871' stop-color='%23bc88bb'/%3E%3Cstop offset='.921' stop-color='%23ae6cab'/%3E%3Cstop offset='.965' stop-color='%239f4d9b'/%3E%3Cstop offset='1' stop-color='%23932a8e'/%3E%3C/linearGradient%3E%3Cpath d='M45.2 1l27 26.7h-27V1z' fill='url(%23SVGID_4_)'/%3E%3Cpath d='M45.2 1l27 26.7h-27V1z' fill-opacity='0' stroke='%23882383' stroke-width='2' stroke-linejoin='bevel'/%3E%3C/svg%3E");
	background-repeat:no-repeat;
	background-size:contain
}

.icon-mov {
	background-image:url("data:image/svg+xml;charset=utf8,%3Csvg id='Layer_2' xmlns='http://www.w3.org/2000/svg' viewBox='0 0 72 100'%3E%3Cstyle/%3E%3ClinearGradient id='SVGID_1_' gradientUnits='userSpaceOnUse' x1='36.2' y1='101' x2='36.2' y2='3.005' gradientTransform='matrix(1 0 0 -1 0 102)'%3E%3Cstop offset='0' stop-color='%23e2cde4'/%3E%3Cstop offset='.17' stop-color='%23e0cae2'/%3E%3Cstop offset='.313' stop-color='%23dbc0dd'/%3E%3Cstop offset='.447' stop-color='%23d2b1d4'/%3E%3Cstop offset='.575' stop-color='%23c79dc7'/%3E%3Cstop offset='.698' stop-color='%23ba84b9'/%3E%3Cstop offset='.819' stop-color='%23ab68a9'/%3E%3Cstop offset='.934' stop-color='%239c4598'/%3E%3Cstop offset='1' stop-color='%23932a8e'/%3E%3C/linearGradient%3E%3Cpath d='M45.2 1l27 26.7V99H.2V1h45z' fill='url(%23SVGID_1_)'/%3E%3Cpath d='M45.2 1l27 26.7V99H.2V1h45z' fill-opacity='0' stroke='%23882383' stroke-width='2'/%3E%3Cpath d='M6.1 91.1V71.2h6.1l3.6 13.5 3.6-13.5h6.1V91h-3.8V75.4l-4 15.6h-3.9l-4-15.6V91H6.1zm22.6-9.8c0-2 .3-3.7.9-5.1.5-1 1.1-1.9 1.9-2.7.8-.8 1.7-1.4 2.6-1.8 1.2-.5 2.7-.8 4.3-.8 3 0 5.3.9 7.1 2.7 1.8 1.8 2.7 4.3 2.7 7.6 0 3.2-.9 5.7-2.6 7.5-1.8 1.8-4.1 2.7-7.1 2.7s-5.4-.9-7.1-2.7c-1.8-1.8-2.7-4.3-2.7-7.4zm4.1-.2c0 2.2.5 4 1.6 5.1 1 1.2 2.4 1.7 4 1.7s2.9-.6 4-1.7c1-1.2 1.6-2.9 1.6-5.2 0-2.3-.5-4-1.5-5.1-1-1.1-2.3-1.7-4-1.7s-3 .6-4 1.7c-1.1 1.2-1.7 3-1.7 5.2zm23.6 10l-7.2-19.8h4.4L58.7 86l4.9-14.7h4.3l-7.2 19.8h-4.3z' fill='%23fff'/%3E%3ClinearGradient id='SVGID_2_' gradientUnits='userSpaceOnUse' x1='18.2' y1='50.023' x2='18.2' y2='50.023' gradientTransform='matrix(1 0 0 -1 0 102)'%3E%3Cstop offset='.005' stop-color='%23963491'/%3E%3Cstop offset='1' stop-color='%2370136b'/%3E%3C/linearGradient%3E%3ClinearGradient id='SVGID_3_' gradientUnits='userSpaceOnUse' x1='11.511' y1='51.716' x2='65.211' y2='51.716' gradientTransform='matrix(1 0 0 -1 0 102)'%3E%3Cstop offset='.005' stop-color='%23963491'/%3E%3Cstop offset='1' stop-color='%2370136b'/%3E%3C/linearGradient%3E%3Cpath d='M64.3 55.5c-1.7-.2-3.4-.3-5.1-.3-7.3-.1-13.3 1.6-18.8 3.7S29.6 63.6 23.3 64c-3.4.2-7.3-.6-8.5-2.4-.8-1.3-.8-3.5-1-5.7-.6-5.7-1.6-11.7-2.4-17.3.8-.9 2.1-1.3 3.4-1.7.4 1.1.2 2.7.6 3.8 7.1.7 13.6-.4 20-1.5 6.3-1.1 12.4-2.2 19.4-2.6 3.4-.2 6.9-.2 10.3 0m-9.9 15.3c.5-.2 1.1-.3 1.9-.2.2-3.7.3-7.3.3-11.2-6.2.2-11.9.9-17 2.2.2 4 .4 7.8.3 12 4-1.1 7.7-2.5 12.6-2.7m2-12.1h1.1c.4-.4.2-1.2.2-1.9-1.5-.6-1.8 1-1.3 1.9zm3.9-.2h1.5V38h-1.3c0 .7-.4.9-.2 1.7zm4 0c.5-.1.8 0 1.1.2.4-.3.2-1.2.2-1.9h-1.3v1.7zm-11.5.3h.9c.4-.3.2-1.2.2-1.9-1.4-.4-1.6 1.2-1.1 1.9zm-4 .4c.7.2.8-.3 1.5-.2v-1.7c-1.5-.4-1.7.6-1.5 1.9zm-3.6-1.1c0 .6-.1 1.4.2 1.7.5.1.5-.4 1.1-.2-.2-.6.5-2-.4-1.9-.1.4-.8.1-.9.4zm-31.5.8c.4-.1 1.1.6 1.3 0-.5 0-.1-.8-.2-1.1-.7.2-1.3.3-1.1 1.1zm28.3-.4c-.3.3.2 1.1 0 1.9.6.2.6-.3 1.1-.2-.2-.6.5-2-.4-1.9-.1.3-.4.2-.7.2zm-3.5 2.8c.5-.1.9-.2 1.3-.4.2-.8-.4-.9-.2-1.7h-.9c-.3.3-.1 1.3-.2 2.1zm26.9-1.8c-2.1-.1-3.3-.2-5.5-.2-.5 3.4 0 7.8-.5 11.2 2.4 0 3.6.1 5.8.3M33.4 41.6c.5.2.1 1.2.2 1.7.5-.1 1.1-.2 1.5-.4.6-1.9-.9-2.4-1.7-1.3zm-4.7.6v1.9c.9.2 1.2-.2 1.9-.2-.1-.7.2-1.7-.2-2.1-.5.2-1.3.1-1.7.4zm-5.3.6c.3.5 0 1.6.4 2.1.7.1.8-.4 1.5-.2-.1-.7-.3-1.2-.2-2.1-.8-.2-.9.3-1.7.2zm-7.5 2H17c.2-.9-.4-1.2-.2-2.1-.4.1-1.2-.3-1.3.2.6.2-.1 1.7.4 1.9zm3.4 1c.1 4.1.9 9.3 1.4 13.7 8 .1 13.1-2.7 19.2-4.5-.5-3.9.1-8.7-.7-12.2-6.2 1.6-12.1 3.2-19.9 3zm.5-.8h1.1c.4-.5-.2-1.2 0-2.1h-1.5c.1.7.1 1.6.4 2.1zm-5.4 7.8c.2 0 .3.2.4.4-.4-.7-.7.5-.2.6.1-.2 0-.4.2-.4.3.5-.8.7-.2.8.7-.5 1.3-1.2 2.4-1.5-.1 1.5.4 2.4.4 3.8-.7.5-1.7.7-1.9 1.7 1.2.7 2.5 1.2 4.2 1.3-.7-4.9-1.1-8.8-1.6-13.7-2.2.3-4-.8-5.1-.9.9.8.6 2.5.8 3.6 0-.2 0-.4.2-.4-.1.7.1 1.7-.2 2.1.7.3.5-.2.4.9m44.6 3.2h1.1c.3-.3.2-1.1.2-1.7h-1.3v1.7zm-4-1.4v1.3c.4.4.7-.2 1.5 0v-1.5c-.6 0-1.2 0-1.5.2zm7.6 1.4h1.3v-1.5h-1.3c.1.5 0 1 0 1.5zm-11-1v1.3h1.1c.3-.3.4-1.7-.2-1.7-.1.4-.8.1-.9.4zm-3.6.4c.1.6-.3 1.7.4 1.7 0-.3.5-.2.9-.2-.2-.5.4-1.8-.4-1.7-.1.3-.6.2-.9.2zm-3.4 1v1.5c.7.2.6-.4 1.3-.2-.2-.5.4-1.8-.4-1.7-.1.3-.8.2-.9.4zM15 57c.7-.5 1.3-1.7.2-2.3-.7.4-.8 1.6-.2 2.3zm26.1-1.3c-.1.7.4.8.2 1.5.9 0 1.2-.6 1.1-1.7-.4-.5-.8.1-1.3.2zm-3 2.7c1 0 1.2-.8 1.1-1.9h-.9c-.3.4-.1 1.3-.2 1.9zm-3.6-.4v1.7c.6-.1 1.3-.2 1.5-.8-.6 0 .3-1.6-.6-1.3 0 .4-.7.1-.9.4zM16 60.8c-.4-.7-.2-2-1.3-1.9.2.7.2 2.7 1.3 1.9zm13.8-.9c.5 0 .1.9.2 1.3.8.1 1.2-.2 1.7-.4v-1.7c-.9-.1-1.6.1-1.9.8zm-4.7.6c0 .8-.1 1.7.4 1.9 0-.5.8-.1 1.1-.2.3-.3-.2-1.1 0-1.9-.7-.2-1 .1-1.5.2zM19 62.3v-1.7c-.5 0-.6-.4-1.3-.2-.1 1.1 0 2.1 1.3 1.9zm2.5.2h1.3c.2-.9-.3-1.1-.2-1.9h-1.3c-.1.9.2 1.2.2 1.9z' fill='url(%23SVGID_3_)'/%3E%3ClinearGradient id='SVGID_4_' gradientUnits='userSpaceOnUse' x1='45.269' y1='74.206' x2='58.769' y2='87.706' gradientTransform='matrix(1 0 0 -1 0 102)'%3E%3Cstop offset='0' stop-color='%23f9eff6'/%3E%3Cstop offset='.378' stop-color='%23f8edf5'/%3E%3Cstop offset='.515' stop-color='%23f3e6f1'/%3E%3Cstop offset='.612' stop-color='%23ecdbeb'/%3E%3Cstop offset='.69' stop-color='%23e3cce2'/%3E%3Cstop offset='.757' stop-color='%23d7b8d7'/%3E%3Cstop offset='.817' stop-color='%23caa1c9'/%3E%3Cstop offset='.871' stop-color='%23bc88bb'/%3E%3Cstop offset='.921' stop-color='%23ae6cab'/%3E%3Cstop offset='.965' stop-color='%239f4d9b'/%3E%3Cstop offset='1' stop-color='%23932a8e'/%3E%3C/linearGradient%3E%3Cpath d='M45.2 1l27 26.7h-27V1z' fill='url(%23SVGID_4_)'/%3E%3Cpath d='M45.2 1l27 26.7h-27V1z' fill-opacity='0' stroke='%23882383' stroke-width='2' stroke-linejoin='bevel'/%3E%3C/svg%3E");
	background-repeat:no-repeat;
	background-size:contain
}

.icon-mp3 {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAnxJREFUeNp0U89PE0EU/ra7XWxpSsFYIbVQf9REFBHkYBRIPJh4wrN3DsZ4MPGP8b/wUCIHEw5EY0w04o9ILcREGmwVgaXbbXdnd2bXNxPahGyczebtzrz3ve99740WRRHkWn5cebu4cH6SMY7e0jRAHr9c3WxsVvcemmbys9yT6+uHJ8oaPefypdPDD5Ymh5w26wMkEho8JtDtuEOZFCrvN/4uJZNGH0T59D58X/C27aFNAL3Xthmsww5GCyN4+uzu+OLtQsUPxPQx6ZMAoQjBAw7O+bEVCMMQgqygs+LFs1h+dGd8bna0QmXO9OL6JYgwAvOFZKKoy3V44CgNfv7Yx8oLH+lUEgvzF8Ydhz+n41snAGRG5gUEwClzhHdvttFxfNyYK0EnJozKK5eGcf1qHo1GOxtjwI+pfvm4g/W1qtJgerYE2SXJSIL9+W0jk0mCShAxDXgQKgbNXxZq35vQKCiKQkSUXdc1+gcch1FHGPmKuIgBCdc66qJQHMG9+1NIpUylxxHtuW6gEiTIu+N4yjdWgty0yTmdNjFzcwKjY0MU7MLt+IjoSad16FoIx3b/A0DZ7FYXnsdpAjUMDOjI5zPgfoBsRodhhGhZHfBBU/nGAGRtxWIOg5lT2NtrI5dL0SB5KJzLodloqXaOEatPGztKq5gG3S5DNjuAK5NjKJfPYKI0okBkSdemCiSgS/rkQNLSePtxBj4LSCwfFtE0krqqX7ZVMnu9XlMXy2l7ME0dzA3iANQyY6vWxC61UY41zTyNcYh6/QCNXQvzi5dR39nHVq1BUyuMGAARsF6tbbe4iKD1r7Om5iFBdmW1SsDflLiuB6sX90+AAQDHAW7dW0YnzgAAAABJRU5ErkJggg==);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-mp4 {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAnBJREFUeNpsk99r01AUx79psrTrujVtbceabnZs4DYRHSoMh6Dgq77rn+AfoA/+If4Bok+C0CfxVRDBh+I2NqZzrpS1DVvbtU3SJPcm8SSlsJlecsn9dT73nO85V/B9H0H78OLdt/LDlQ1uMYybIAgI9n99OWxoe83nkiz9hDDae330JvxL48O51Xxm/enNtKPbVwAh0Ec6kYpXat9Pnl2GBC02HrjM5Y7h4P8+7FtIFVJ49OrxUnl7ucIdfhv+BIDv+fBcj7p/tXMPrs2RXVTw4OX2UnFTrXCbbY7tpMsA13FDSDAOQ4gJEGUJLs0PPh9CkESsPrmxxEz2lra3rnpAt3G6adgdQhBpmeLkFodNmsjpOPoXBrQTDcmFFNS7i3MRDzzPCw/vva8ikU+COQxm14BBhvJcHLGpGPTOAJxxeLbrRgAkYujBdH4G5oWJWXUW19YL4XqunAMFhnq1BqWYgaY1MAHASQOiU96zKzkU76mwehaOvx6h9uMv7KFN3RopL4oTAI4HRh4wSl399xla+00YbR3yrIzM9SzSqgJJnoKcklGrH08CcJjnBtLLCsSEGGpSWJvHtDKNoFippsJ0ulIsDDUCCATMlBQkNuahEyiZTcLsmFBKaQxaOk53TlHeKkM70AjAooCghBOk9sKtIvqtPqS4FBaRnJSRX8tj2DOh3lFB5Qw2ZNFK5LRo6w4sKt2ggAzywidAMN/9uIPSZglBLDO5FF3mRD3wHE9qVRvoHrUpfn+UEQK0/7ShtwboHJ6jdH8RZxSC57hSVETb7e5/2u0FxqPHJow+8iZ4lYY2QGu3idhIxO7Y7p8AAwALCGZKEPBGCgAAAABJRU5ErkJggg==);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-mpg {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAnxJREFUeNpsU0tPE1EU/ubRdlqmnUBboa0UeUQDiUGCC1+JmrhxoXt/gBvXJi74If4AV0Y3sNKF0YUaICqoIfjgVShEiGF4tDOdO/fOeOaSKtie5GZu7pzz3e/c7ztKGIaI4vn9p+/P3h4e4a6Pv6EoQBDiy7P5rc1P1Xt6XP8M5ejXo6UJ+dWbuemeTGdpvNdiNe9YvQLe4Bi4PmTpRmyq8m71rp74BxKF2twIHvAo+f/l1T2Yp0zceHizfOZa/xRnfBRhG4CQqAYioBWeXDyA8Di6ei1ceXC1XBwrTXHPH2vW6ccBBBMI6BsSUEQzakGL6xB0tvjyBxRNxdCtc2Xf8R9TyaWTDOg2TjfVdw6hqIoE9B2GxkEDWlLH7s4ette2kSp0oDRezrQwCIIA3oGHr0/mKMmE53qo23W4+w5S+Q5ohob9X3tgHgO8ULQACC7gMx9mKQP30EW6mEHpYi8xcJEdzMucjfkKcrTfmqmiFYBxCF/Id+gayKJwoQjHdrA5v4HK7Cq44KjZNWpagaqp7QACks0H9znW365ia24DzoEDozOJbH8eVtGShXHTwNracnsG7q6LzsEuaAlNPm9h7DSSVjLyCMkppDI+GS2StQWA1RlKo0X56n2X+6QHkmkDakxF9WMVqWyK+s/BrthYfvWz1Ug+zUDcjMPMm0h3pxEjFma3CbIuCud7oMc0LL1ZgmElpGJtW3B+15HIGNITrMYIlOH7i0U41NrInREylYbu4R5qQbQBaAh95fVKZCnpQCnb9DrWZyrRERS6NDeUw+yHaXh7rt4C4B8y+9vkwn7kwKNRpDoa9aiFKBYnF+RcREqQ2e1m3R8BBgAy9kz9ysCE6QAAAABJRU5ErkJggg==);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-odf {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAi5JREFUeNp0UktrU0EU/mbu3FfE1KRRUpWYheALNBURUVy7cy9UkO6KW/+Lbt0IPsFui4gLBbUqFaUuXETUKCYa0jS5yZ2ZO557b5MmTXpgmDPnfOc7jznMGINYPi0de5UvmpORxpjE/kbNqW005DVu8TWw1H758ZfkFgNgJmtyxSPRjJIj0QTW/RDiYGXGb7Dl32/eXrVsd0gSCx9miqC0ooCdp69g5Q/h6OLN0ty5ynIkwzMwUwh2FwMdcbDiCZQXlkqFCpEoPT/wih1YjLInANcD+/Ua9bu3wJlGvrBZCmet2+S6ME5g4oGlZ9A/I70XCDhhDexPNTFmswJBwcnuXkF86VSNZxVu0ukLSGnBcqlnN4HoCQIaIuIv7LUooMOgQ7q75LAAb59B9gCBHSKgqemRr94mMKmD24CfM8nb7THYGQNLpAkUkcb66JyGBFFEWRVL57gFEH5qj8Lxwca2qS3EZaugmzAw24dR/XQgwtsCSBjPIdWbUoE2UJLBnV8Ac/ciWHsK9/glWLnD6K2vgPszsOdOQdfeQ1c/ThKoTgDn9A3KUED/52d45xchZsvorD6Bf/Z60riV3Q9Z/0bbGU1uopYGkfERSQ3VbsMwl0qlqoIARmSoPYXWy0dor79LfBMEEd8jGs/uQ3Yl7PJFNFbuEXiV2riCf88fovXhBbo/vqP3t02/ZYmJFqTkzY160Go9uEMbFK8hR/NrdXtFuUVmnmySVGgO4v4LMAAjRgmO+SJJiQAAAABJRU5ErkJggg==);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-ods {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAetJREFUeNqMUj1IHEEU/i7u7Z23e8tGgneGQPw3hZDkkhQiSuwMQREba4uUgpVlCrvEQhurkCoWqcQQ0oTAaYKNqJygGEwgHCSB6Knn7eXcdX/GmdHVPWYFP3gw78173/vmvYkQQsAwNvckq96UnyIEh7/d4t7uUd/8y+85P+bXSX4grkhI6nJYPW7LrXpBK2YxiSoShhu4Buq1NPofDeqdrZ3Z4cl7D4J3UtA5VyVAlmJoru9Af2ZAp1lcCQ3nqgiuKmbY3l/BH+MnHM9GVLP0Ww3KNA33CQoQQnL834Fj74PUGkANEIkCSSsa8gQqgYTIcB0PVsXB318GInRiCVWCkpRFAs+j5gKlA4t29Ggh4d0t04FKt9PQqF4UFgumSEA8ApeaElilWbYRVy/lsns/N1QBkxtENF4jxPxcgcB1CZVOrvMteK5IQDtJJIGh++PcX9iYwWjXK37+vP0WdYk0Ht99jtX8JywWFkQChw4tc+cZcvlF7rMze+ubbxN40fMalRMDP/6twaiUeK7wlZ0TD0a5hLTWxo2d45KKprqHKJslTsy209s2wnMFBTYNZjc/oLt9gPvLOx+hxVJIKS2YW5pCbSyJTGMK775O8VyBwDJd2LTDl/X5i8v3S7NVw9vJb51tITDEUwEGANCx2/rXEEFFAAAAAElFTkSuQmCC);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-odt {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAepJREFUeNqMkz1II1EQx/+7Ca6JkqyYiJ8cKEpAQbBQFDm0sVOsFBS9wt5KOTgEG5twxVlZ+XEnKNiIghYKxx5nwEpIIXaiSAgKGmMi0d23u8+3T7OaZJEMLG9mmPnN/w1vBUopLPNNhRWXHOyDg0nx82TiJtZPlPVoNpftc2cTotcHtxx06kdXpSQ/BvzKESZzIDmAz6y+NojOjpDMZiqRPIgNoFyWM8DrKUV7axO+gcp4g7AzmquAdVNqOgL2z2I4id1B0wgeygOyt/rLL5buLwAIDgA9dY+L+DkuDQOCrkMgBsRglcMOqAGwIstMg8AkGsuZMNUMRMkLqE+QGloglvlA7uIOAKvZajR0qJkUj/XHe0BTIclVKKlrfKsj9qA8gA6wqSJzPaXlr7ky//tdLEUfawsBjExUFGVWbT7AxSa42H2LMfODmvd3wKb7RAMLYwM8nts8xJ/pEe7/3PmP2eGv3D+9usb35W0bINoA7RmjXSHsH0f5Z/mUSZ0Ir2JmsBtD80s8/rGyzWsLFTD5yUQCbfUBHl9d38LvkdDTXIuHVBo0k+bbt06qO+yAPGXwe/cA4wO9PN44jKDG70GougIzi2tQ00ms7/3lpwnBBgjZ37Kkd1Shht5XzBIFl/ufFtniT/lFgAEAU//g6kvdGBMAAAAASUVORK5CYII=);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-otp {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAcJJREFUeNqMkssvA1EUxr+ZjkdbrfFKVD12ErYSRELY2fkH+BMsLcQaSwsrSzZi47EjJEQkEhYkFlhYSVtFpdqOqpk717l3jKZmiC+5mZlzv/s795wzCuccQncz3YeRBj4KHz0/RrOZe2NsZPP20o255zQ3EAxzEAC+6uzTw13G4TFQAakA/CWtIYbY0KBOrx7IvwDQqlHV1o3YxKTOvyAUvfQCfqmA3e4ikyS/zRAKvOot7eoSHEgZIHrCfQAfBqBaKQQDKScQAExd8emBANg+2U2CvNMkkgSqBmrCxFB8mujeoJBWwEqARcssKTAJEGrmaGrjqK1zvNknH4BtyxKl2VUpRxmj5W+x73q9AEaZrR/ND1EJluIpS3i9JQiA+a+hSq8HwJjTsLrRaWitPTCOlhEZn5N75sM1qigmlN+dB3u++Qao5W4TtbEXXIsiszGL4PA00itTsu6XnQWo0TjMTAJqfMDx/ryBJcaVzSNSH4fW0Q+rkIf5rsjRiid7yyN7uoXS3Zn0egE0NiORAN9bQ017D1Lri7CLlP2EDr3Rf7C/itzV2bfXA/igLDaRixfngFhSCooH2xVPCWBlwKcAAwBX1suA6te+hAAAAABJRU5ErkJggg==);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-ots {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAfZJREFUeNqMUk1rE1EUPS8zmabJdDKB2glEwY9ExJYiBUEQpV25qgtBXfgbpEtXuujKf+AfEKRddOdOGHClbYVCvyKWaijT2mhjphk7Sd7Me76ZONp0EsiBYWbOvfe88+69hHOOAE9f3zTVnDKNHvhlsfqPw/rM0ovyWsRFdXJEpDIyRnSlVz0KSkmvabaJeXSJBEhgAJzTDNybmtUnS5Pmg/lrN07H5NM/f13FoMgpXDSuhiIiK3Qi6LUugX7FAbaPPsJqfIHHKCStqRsXVFPQuZgD9BBxjikSiRq41AAkgCQBzVf0+BWEBX7GBm0xgHHUqk1UbBuEcIydzyCZlOI9YEGuDxwduCCitS3Xh3viCZ4jrcq4PJ6DLHd67tjtuAAXib54dCPVEfQ5XIcik/0/2iDeOYz3ceCxrisMi904y0XiMQFfkB7lg6xFHwFxEqUMV0anUNBLWKm8xd3i4zBWOzmASx0UsiW831mA59Xjm+h7HCOygduXHqJatzA7Poey9QnXjTuoVD/j/sRcmDOWLgqnLC5A2wwST+Pn8T629lahSCo291bwu9XA7vcy3m2+gTaUR14thrk9BXasbdiOjSe3nmPpwys0xSi/HpbDd3bIQC6dx/q3ZbRb/j8BEi3Po5cTJpHI9CBNDEa++GyDBN9/BBgAwfDlCVUQaNAAAAAASUVORK5CYII=);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-ott {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAdFJREFUeNqMU89r02AYfpJ0iVm7EqhVOxw7dDBEdpiCE1RoEZRddvUgbIex/Rs7eehppyF4LOzQu4MxwYp0HgShIuwwUVSCVtl0s13afl+SzzcpyZYmyF74eN583/s+PO+PSEIIeJZdrtQVI19Cgmk/Ph39bpllXq82g7sgLxVcyKNZpIx8Uj5u5zSjc9Gov8ZihCRC8D+7On4JczevGeTGSEIC4ctKJtB1DTPXi1iCCEkIm1EFlC2Em0iwtWfinXkIzjiO0jljtDC5TtflGIGUQMB+mfja/oPv2Rx9MMjpMdJxOXyXTwkcwIkewfqQ1QtQNB385zcI14FrtQexsSb6SRysZ4Fbf+F6eHwATc9gJGNAm5iCTL5n/LCVRGADNoeaGoHqyaXj5gqQlTODovcwNk5Aj6wXqV8eCo7EDhMonEHpW+dZC7gUG98D3geo7vkb01h9cAvPdt76OGy1xntUd3bjUxAk3+l2sHJ/FgtrT0MUJNfDSm0bjQ/72Hzxxo+NK+h3B7XRNO4UrwymQtMIkdTBU0m+sBOayLsn8Ka78mQDjx/e87HXPkb1+UsfP37+AmZ1fP/suknBb6nefVQXjl06TxMlJfWKNWr+Kv8TYAAkUueexJF47QAAAABJRU5ErkJggg==);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-pdf {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAmhJREFUeNp0U0trU0EYPTP35qYxaW6TlDapNKWGbgo2FkF8rARB6rboXusf0F/hyq2U4krFqugqSBeuAyL4SERBstHa0iR9JKZJ7mvu+M0tqZGkH3x8987jzDnnm2FSSqh4ns0VU1ybFzj674Wa3uWiWbfsFQb+jrGj8Xvbm0HlvYVRxhJprpmTlGmum+OMm5uNPZNbtjk3l82ey8++8oW4Jv/H/wdA456g2kvH99FyHNiuAz2dwflbN8YW8zMK5Go/CMfQkAhpGsyQgRCtlpE4jIULyC9fHzu7MPPEl/5ib6WOE0JJNRiHHg6j86mMjw/2gG4bkbY4PW4Yj2j64skA5FTHdaEMPiAJszt1sK0d4suJmY4k0+IDDGRfqmh0u5gejQc+fG8eYCIahRQCEfgQnIuhEkgtONE+dGxYxEDj1DhiEycZ+1YXdUpHCqTMJIYyEES5aXXQsi2kYlGEia5GtHVKn+amPBeCutPgfLALPuVu+xDVPw2EQyFEjHDghbpYNm1yKVVnYjTOerepn4E6XQmLGSPkPkOXWATMSDcjQEkAaqOu6+i/rccALtFL53LI3r0Nq1ZD4/MXZJaWYFer+PXiJc6s3IEgY3+uPYZHTAcAHM+DTE8gnM1CSyaCulv+GrRy8uYyElcu4XfhLVpkpNtn/DGA5Uu0abFH36WnzzCayWAkmYJvWeCkfb9SwY+NDbSoOx4bYqJF8rZqVRRXV/HhzWtUSmWwmWl0RmN4v76OUqGASrmMOkntSHF8MOs954dT08W248wzYsJDOujRBAaqqikTpRo/qqd0/dv97c3Lat9fAQYA4z8bX9nTsb8AAAAASUVORK5CYII=);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-php {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAhNJREFUeNqMkltrE0EUx//ZbDaXNrvZzdIkbYOXGgxYQlCK2IIY6EufxGdB8Av44AdR8AP44JOPBR+Ego0PClUKTTXQSmkTYtOkmubSJrQ1e3H2yJSEJNIDs3PmP+f89pyZcdm2DcdWvn7LzkxFHmCIra7nm9ulg8yLZ09yXON55Dgjt1PM2iPs0+aW/frdh8bzV2/SvQBnCLiEqcFxLKSSodlrU9leiGPihWePBkgeEZO6ShC2dCAZNuf6ADb+ldQ5PUPx4BCFcgXfdwq4Ph1Dtd5CZi4Nw7SQiMdCXkl6yVIy/QBWgcU+yx/XsLK2cdHndqlK/lZxH/OpJO7fnsWY3z/YAq+g0TmHpoUH2vB5PXi8RD9Fo10aAmDJTgWyIuOupmK38rsPcOvqJO33XWEvwLJsmKxHRVEwf/MKWl/yUMf8mIloWN8rw+sP0D6PHQmYuzGNgCRiMZVA17IQV4OIaTI8buH/AJMFd02Tkp05PO4jnWvc57EDAINt7u1X8Pb9KgI+Lxbv3cFR8xjx6AQ+b+Txs/qL9KePlih2CMBCq92hg2qzt1AoV7H5YxdhdqhHzRbgcpFeqdUplpvQW4FhmAixZ/sws4BoWCM/qmsE5XqE3dDQCrqGAYWdejqZgK6GUD8+IV9VghBFN1RZJv3sT5diBwC15gncggCPJKF0WCPN8dun55jQdVpz3Ynl9leAAQAJhiGatD9AOgAAAABJRU5ErkJggg==);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-png {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAmtJREFUeNpsU9tOE1EUXXPp0CAUWmJbC04xBANNTF+kKhG8fID6aqL/gPEj9E0lIf6Dj30HL03wxQtVIC0QKrWxNG1Dk9Z2Oj1zxn1m0oIZTnIyZ8/ee+211z5Hsm0bYg29fLGpxWIJWBYGS5IA8ncKhT9Wvf4Yqprtu+w3q85X7f9QxseD/pmZMZsxN9fnc5JNw0ACGGv6tPSvyvEDKEoWZ5Y8OHHObKpucw4B0t3agnl4CJPs2YkQVu4s61ORaBqMJc8CDBiIRhhVM9bXYdVqYAcH8M3NgS0tQQsFcfdKHEbvlr6WyaR/V6uPKPy7B4DT7lUq4MUipMlJ2MPDUKtVfKZ2nn/5BoNbkONxXeb8LYXe/A9AJLNWCxgdhZJagDI9DZg9qIkEytRSkdqTSFQtGILSbgc8LViM+tc0yPfukzIyOJ359k9YR0eQdB2KmBbpwXoM3Dod1SkD+scpEapCI5DdpsJhIJcjajQZagcjI+5oLe4VkeQnyiZgdIH2X6BJ7dSqQLfrggjw0AQwP+/GegCIHppNoFAgEMO1RZKo7BQgRi3yN05cnwdA0BQMAgF3C6pnbuNg92M9AFT1diSCh6kb+FGvo2MxnBB9ocZxp4Mns1cde213B81e7xwAcl4jkaa0IUSjUdLJwkL0Ej6VSvArCt7l81iku6GrKnYEU89VJlSJRmR0Dax+fI9suYxSo4HlWIw6M3FBlnD9YhiXabyOsOeIqG7TzDeIYo6EDGp+ZPb2kKKqH8h+mkxiI5/D1/19J3bwYPvPWXq2skkiJVxesqt0XzghpKM8nRVV2Lv2q9eLIvSfAAMAaacnllcFBmYAAAAASUVORK5CYII=);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-ppt {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAkhJREFUeNpsU11rE0EUPTM7ySZpmzT9DNamWAtFfSiCigr+AxF9zKtv/hvf/Aki+FEi6ov4ItWHPGiwiBUKoUqqTUJImmR3M7Mz3t0kNe1m4LIwc+65595zlxljEJzdR5uf5nLmsvZx6gSvtd9W9bjhF7jg5dH9nRc/wq8YXaTSJptb0xklx7IZoKUEz1zJ2DUU69/37vFYrDxegJ9U0lC+AoIIVGg9CL+vIObP48KDQn7x0sWiVnJrnEDg7KGk+i/Ac4iUM/R7BsmrSSxtXMfa3X7el8+Kjf3KfUJ+iRJQw4w0Tc8BRyWGRAZY3rBR/VlC+XED2ayDhZyXl03+hNA3TxNQshlGLAnE44zCIL1goXZwiMNvB1i6zbC0KuAsxNITWwgNMYPeLVJiFEO9ArjHAivrAjNzBr4f4vwIgdGD4YUACsZCE8AtYGWT5jCsGQw5wEYJzP/pj5RwYTA1b07eQmfZ8P0sgdaM2FlYwWkMgMpl6NQAO33GKM0wsQWflkh1uqGVmVWblsiDkQyqxwfag35SqcktaEWTUTHYNx4iGU/C29+BvX4Lpu/C7zYgFjegSY63WySsHyXwpYHU00ieu0bAOuJbBTArBkiXKiaAmTzcvRJUV9E8rOgqBwqlY8ASs/AadbRLb8CzeTjVClqft6FdB17tL7yeCbFRBYoLr6vR/PiSEl5BZJaBD0/R2nkOZqfQ2fsKt+0SEQ+GLSIEUvJm+6jbah2+pS2aon+4g/afd4SYJVuA7vvXdC/IHQtSoTnK+yfAAIEaId1m+vudAAAAAElFTkSuQmCC);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-psd {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAqxJREFUeNpsU01ME0EYfbtdKKWGtoItRWgJHApCBE2I0YuoiSaaeDJeOJh41YN3TfTixcRwMfEk8eDJGA+Eg0YTTRRMg02KKFooCBbTlkJLS7f7P+u3K9Xo8iWT3Zn55s173/uGM00TVlwZfzJztD92iKO5ouvQGQPHcQDN380vlDPr65fdLj4Oa41i9sFt+ytgN7o7woGOrqgvvpLBaF8vWj1NUAwGTVNRM3mf5vU/zaU+XySQuTqIFXz9hxmGLkoS7r+YxvVnrzGzlgXPDOzUZPT4m3Dt/KlIuH9oUjXYEHZZ/wOgGQZi4TZcGI5hLb+FO++TSOSKcLtcMA0dI0EPrp4+HtnfG5skiUecDGwQE2MjAwiGWlFVNDz+tIyCokJhPKYSX7Gdz2I01hOJdnY9rJ/7UwPGTEiqjtbmJtw4MYx78S/4Wa3h5UoOYwPdIOp2Xi/t18rlFgcDw6o+ydiWVRwOBnCpL0oOAMmNEhLZIgSeoxwGSWcERon/M9DoBknTIdNQNAMnO4PIVGpIFXcwndlA2OtGc4MAxml27p4AIulWSIa9QVadiYSoJxhqBJivKgh5ad3k9gaw6JdlDaqq7q5wINY4F22HaLHSDZQkBW72O9cBYFEviBIURQH7a7MN0uDisUW12ZZcaGlmdq4DwCqeTo1zNtZuW7hUqGIw7MNqSUS2ImNsKEpSdEwt5lGhfQdAkQBEoub3NNrDJfAIeBuRrcrY5xGQ2RFJAjl00I8PCckJUCB9q1URBnk38XEJEuk41tmGwZAf66s1VOh2keqwoUnYpFxHH4iKIixkN3HzVQKP3iQR/5GDKMuYmE3h+fx3MHqh1sMafztHLuiCg0FAk0uFdLqcpGY5QEXbTC/j7mIaVjc18DxufUtBJ/vcggs+3ijVz/0SYABsJHPUtu/OYwAAAABJRU5ErkJggg==);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-py {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAlVJREFUeNpsUktvEmEUPTPzTUFmgJK2UqXQFG3pA6OBLrQxamJcaYwuu3Dp0l9iXLvVtRuDpgt3JIYaTVSaxtRHsJq2xEJBHgXmifebMhECXzKZme+ee+65516h2+2Cn2cb2VwyHl12//vP2/zOQaF4uD7GWN69e/LogfNm7kUsPBFaXYwHMeK0OlpQEJApHJTuykzK98dE98O0bLM/UNgr4v32Dj1fwSQRt9dSsfmZcMa0rIv9ODaqYrPVxuPnL1Cu1aEbJu7fvIZUIo4bqeVYRzcyv/8c3SPYpwECt/dmu4ON3Ed4TymI+hQc1ZqoE+F+uQLDsnHlwkKMscJTgl4eJOi9fxZLePNhGx6ZQRRFqH4VjZaGSv0Y6cQcJLpra0ZguIWegqDiw7lYBBZV6xiGk9DQDLzK5bEyF4Hi9VLMsoYI7J6Es5PjeHjnOl5ubqHaaJGBEkzbxplQAKIgDmBHekDTgI+qKKqKLvNApgmEgyquLs1CoFn2Y4cIeLJpkjoCLkWnUSIF3JxISIUsCjAoxhWNJLBIJs3YeXj/08oYZkOKY65HllE/bkMmY504YUd40HUq2JSSyW6iVPmLiXE/ZMYQCU+hXK3h1toqdNN0sEObyKtqtDQ6kXDwcadDS2TBryp4nX2HxXjsJK6bDnZIAZem6Tp5YMMmicn5OC4lztNWtvB9cg+hQABtWjKL2jH/T3GgBcYDXEE6mcDM6SlaJAGMWkivLBC54ZgniZaDHSI4rNSqn7/t1vgkGJPwZXffSeCjk2iUWz9+nSTQN8e6ef8EGAClUi/qoiOc3wAAAABJRU5ErkJggg==);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-qt {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAnVJREFUeNpsU8tu00AUPU5sp41NkzRxpfSZqi0VIIQqEEJUZYXECvbwCWxYsuBD+ABUFrDrCnWBQEJdIWigBSr6pqRJ1ebhxrE9M7aZmSrQ4o505fHMnXPPPWdGiaIIYrx89GKpNDdxmXkU3aEoCsT+z8W1Sm21+jCpJctQTvaerj+TX7WbnJ+0cpfuX8mQtn8GgJ4AZtIFY2Hz3foDVRcgyt+cRHcS0IARh+D/8G0PpmVi7smd0dLs+AIjwTVEiANEYYQwCHlEZyJgIQKfoX84g9uPZ0cHZ4YWmE9nuufU0wABCSSImMsWEgqSuoqA/39/swZFTWLy7vQo7dDnfPvWWQa8GuOV3IYLJXmyzDzG2/ChZ3pwbHdQ267BKJoYuj7SF2MQhiF8LuDK/Gf0DKTBKINz1IbTbEMzU1ANDW7LAfEIQKIgBsBFlAx6LYOz6MAcvoDCtAVGGPKlAiIu/F55F33FDA6W93EOAOMaMOl7biKPwRtD8Foetj5sYPfTDtxjl1f3Ubo5jkQieQ4ACSUD2iE4XDpAdbUiW9D7UsiN9WNkZgxajwbd0LGzt3keAJPUc1N5SVeENT0Ao2BKV6QzwlZeRBSKAYhe3aYHcZWn7l1EfjyPypcK9LQGa8qCvW9j9+MvaasQOHaRhGWdhsNLR8hwodYWf6B4tYjDjSOovRqq32rSYq/lytw4A77o1V2ERiAtzY5kkUrrsH+3QF2KY87ArTtQuQ6nAf4x6FCV1D001+vYersBM2vA4y1Rm2D7/Rac/TZIw4d/6MrcGAPf9htN0miJh7Lyuoyvr8rQeP9iVJcrSKgJ+TrFcyYebXTP/RFgAFQobmIOBxbsAAAAAElFTkSuQmCC);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-rar {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAnpJREFUeNpsUktPE1EU/u68OgylZXi0hZACQU1LEKKCMcat7jTRnQsXxsQtv4E/4M74P1iriUaNCw1FgxpjCJQKKAU60+m8mJnrmSll4XCTc8+959zz3e88GOcc8aq9evChOHl/lvMoubvWX/z4+BwTlbvw7bXdg8b7h6LE1gGW+O88CRMt4XTlR6/rYxce5Xv3jlHH19fPkBu+gWy5mlcFb3Wn/umeKOEMJF5C7xCFbtA9dRXjFoYKGiTRAlPGUV1aKU9O3VwNQ74A8DQAIZxqAuAhBPIMFYpQVAVB4CPSZjEzv1weH5tbDQN+JQ2Abu488mnzIbAAA3o/VK2PwDJo7r5Fy7ZRuvi4PFS6+qIXdVYD8Jg6BUcuOD8BozSLlRWyicgVKkTMQWwUlFF0Ooe5FIPk57BD7G0SiywyjD8bCDyHsOkeeeR3SUxEkROmU6BfQYFJMHfhWXV8efkUrb13VPMTsrcTQSzxZ/+n0GVA6EGbSGdgG9vo15fg2nFgbO8k70SRdd+mahDT81vUxTZRlJBRMsjq89C0EXCvSf7TIBZ136YZUJEiE7LgJ2dN01BZuE0dkIhxE7KcQTK1QUj+cwAEyrPZ+IydzRoyah+mLy2isbWBweESJEnB9q+1RM9Ub9GQOWkABg8HjRr2d9Yh0hTlBlRsfn+D4vg0BvUC9rZqECUJuk7Tzr1zahCYlB6HJAREPwfbbMBzLBzsbUKVI0qBgQkc+SxgWUYaIAqOpKwKXJ6bgGlaaDV/YvHaFNrtDsKTfVSrJeqIg/bRNwjclFIALeP3saybhu8SC4VBHwnhBXXIKocYRXD9QzBi4Xgchmkd9+L+CTAAMqwy+ZzluBgAAAAASUVORK5CYII=);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-rb {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAixJREFUeNqEUktvElEU/mag5f2yJhXLwxIt0kiqsVEXujP+A925cu1Pce3WtXVtYuJCF7KtTY0NrVQIpRVKeXTkMcO9F8+9ZVooJJ5kcmbmfOe733fO1YbDIWS8+/g1dycVX7W/xyO3vdsuVKqvnE7HZ230783rlyo7bVBicSGyfjsVwozomVbIPe/c+FmsPHfoRKJd1HT7hXHBZjVbA4aA14NnD9bC2VR8gwuxPi5Sx39Cp+M0XUP0ahhP1jLhW7HFD4zze3b93ILtXYyyVKlR8/5hFbnvO9gtlrGSjOF+OpXkYviWyo8mCS4R6bqO4p86vm3v4fC4DrPfw4unj1XN6JvBaQtjChzUXK43sVU4wNFJA43Tv/B73edQwTmfIhAjCVL6UdPAj1IVFSKhCdAcAI9rnjBiAjtBYEu3GEeh1sKJ0YXR68sVIujzIhzwY8DEBHZqiLRKkicQDfvABxaiQTc4Y/C65pCOXwcjcmlvJgHtlwi4epYifiQWgmoLZwPW6HQG07LgcOgKO0UglAKOTt/E+09fwAiUWU7QAE9xUK3jbvomsispZVHMVEDSZdHo9rCZ/4VIMKAu0XGjpU7d2S8hk0pCELHEzrjKnCQOYJoD+Dxu1RyiwUm5LaMDo9NFt2cqDLvY4oQFp/QpfT/MrmI5FkWebt+NpWto0j2QmQkOjZ9hpwhqjXZzM/+7LU+cc7lRrjXh8/lVLRK5ovLWXglOsiOxdt8/AQYAzv8qbmu6vgEAAAAASUVORK5CYII=);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-rtf {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAe5JREFUeNqEU01PE0EYfnZmd5FSvgLYFuwWt9EgHyEaox68eDJevHvwJ/hTPHv1N/QgZ2NC4g3kUAQKFKGhjVKqRrvbnRlnht262FHfy+y8877PPM8z71pCCKh4/ebt+rJfXEz26Vjf2mnsN5rPKKWbVpx7+eK5Xu2kyMtNTd5d8MdhiJ9BOO7atFI9ajy1UyAqSPIRMR6ZmoNehNHMMB7fX/UWvEKFMbYKE8DfQnAhwRmmJkbx6M6S5+WmK2Evup2c9yUk2nnKA0XVcSiGXAe1k5beP1i+4RFCXqnPywB/AKVzK34RjHNYlgVKCH50w7EBBogbTa/AVM5SgBdn0gc2AMDjPsbFPz2xye9asweS6n+NTbG8BCCfUtLjff2WoVnVpAH6z6hMUtJE3EykYfpF4vUiL3QNS7FMeSAQRBHW3r1Hq91B+VoBQRji4+ExFsvz6Hz7jm7Yw5OH92AcJKW9G4SoHhzhy/lXbB98Qmm2oCXN5WawsV2TACEoJXqwTKOsb3BtR2ucmZxANpPB8JUhyPnHWDaDpfJ1eZFALzJJ4MKO5MEtv4TSXB7V/br8iQLMz+almRZWbvoo5q9qRlxwewCgeXbe3qrVO5ZkUD/9jJGRLPaOm6COi92TU1DbxYe9umRD0DrrtJO+XwIMABWp9nS+FgaoAAAAAElFTkSuQmCC);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-sass {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAyJpVFh0WE1MOmNvbS5hZG9iZS54bXAAAAAAADw/eHBhY2tldCBiZWdpbj0i77u/IiBpZD0iVzVNME1wQ2VoaUh6cmVTek5UY3prYzlkIj8+IDx4OnhtcG1ldGEgeG1sbnM6eD0iYWRvYmU6bnM6bWV0YS8iIHg6eG1wdGs9IkFkb2JlIFhNUCBDb3JlIDUuMy1jMDExIDY2LjE0NTY2MSwgMjAxMi8wMi8wNi0xNDo1NjoyNyAgICAgICAgIj4gPHJkZjpSREYgeG1sbnM6cmRmPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5LzAyLzIyLXJkZi1zeW50YXgtbnMjIj4gPHJkZjpEZXNjcmlwdGlvbiByZGY6YWJvdXQ9IiIgeG1sbnM6eG1wPSJodHRwOi8vbnMuYWRvYmUuY29tL3hhcC8xLjAvIiB4bWxuczp4bXBNTT0iaHR0cDovL25zLmFkb2JlLmNvbS94YXAvMS4wL21tLyIgeG1sbnM6c3RSZWY9Imh0dHA6Ly9ucy5hZG9iZS5jb20veGFwLzEuMC9zVHlwZS9SZXNvdXJjZVJlZiMiIHhtcDpDcmVhdG9yVG9vbD0iQWRvYmUgUGhvdG9zaG9wIENTNiAoV2luZG93cykiIHhtcE1NOkluc3RhbmNlSUQ9InhtcC5paWQ6MDNDMTBBM0JGMTE5MTFFMTg3N0NFOTIyMTQ2QzhBNkQiIHhtcE1NOkRvY3VtZW50SUQ9InhtcC5kaWQ6MDNDMTBBM0NGMTE5MTFFMTg3N0NFOTIyMTQ2QzhBNkQiPiA8eG1wTU06RGVyaXZlZEZyb20gc3RSZWY6aW5zdGFuY2VJRD0ieG1wLmlpZDowM0MxMEEzOUYxMTkxMUUxODc3Q0U5MjIxNDZDOEE2RCIgc3RSZWY6ZG9jdW1lbnRJRD0ieG1wLmRpZDowM0MxMEEzQUYxMTkxMUUxODc3Q0U5MjIxNDZDOEE2RCIvPiA8L3JkZjpEZXNjcmlwdGlvbj4gPC9yZGY6UkRGPiA8L3g6eG1wbWV0YT4gPD94cGFja2V0IGVuZD0iciI/Po72XUcAAAJcSURBVHjahFJdTxNBFD1bykc/ttvdtttWGgI0bYrUgDZoNYqRJ014kMRXHvwB/hQTH/wFhMREJfFBQxBjhMRIFEQSCAlQxKYGggiU3e3HbnfX2bFt1EU9k9m9mblz5p4zlzFNExYmpue/jmTSZw5PZAl1MAwDT0c7O72wvPdudeNakPNtOZ0tsM7cvzdOc5yN5LDAsTFRAJks/kC2PxFRVe39Si6f4byez62EpAEH/gNN18F53Ri/Ocxf7OtdLMpKT42s/ZPg1cISJp/P0tg0TBzLCoK8D7eHh4RkLLJ4cCz12AjMXwgez8yhqtVo3NbqRKlcxcSL16gZwJ2Ry8KVc8kZO0HdTKlURn+8G6PD2SZhLMQj96WAiMAh2RXFYKI78lcJcx9WYBCycICnpNbojUWpD5Y0C4Zh2D0w6hWc70uQZC+IWfQZrXF0IsHvY+meBd08haAhoVMMQFJKWF7PNZM+klhRyogGhbqxOIXAMOtEwGAqDqVcgbVkkE+5UsEAWavf0az2t0ZqvK2qabh6IU3joizDwTgwej1LdVfJXkdbK8mt2QkayO99A0/0trQ46I1lVcX+UREhnsP34yLp1AD1xibBMuntpzU8mJyi3Tc1O4+l9U06n7x8Q/8PHz1DrrALt8tlr0CrkbJMHTop9Sk5sLa1g8L+ARJdnShKClY3tunN69t5iGLYTlCtakjFY7gxNABdN3B37BaqqoYT8pyX0in4ORbRkIA46YlDRbUTbBZ2Jb/Pw4qiKFnapcpPo9pdbrg8DjAOBsFgELJmsGs7eWkkc5bu/xBgAHkWC6UPADTOAAAAAElFTkSuQmCC);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-scss {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAyJpVFh0WE1MOmNvbS5hZG9iZS54bXAAAAAAADw/eHBhY2tldCBiZWdpbj0i77u/IiBpZD0iVzVNME1wQ2VoaUh6cmVTek5UY3prYzlkIj8+IDx4OnhtcG1ldGEgeG1sbnM6eD0iYWRvYmU6bnM6bWV0YS8iIHg6eG1wdGs9IkFkb2JlIFhNUCBDb3JlIDUuMy1jMDExIDY2LjE0NTY2MSwgMjAxMi8wMi8wNi0xNDo1NjoyNyAgICAgICAgIj4gPHJkZjpSREYgeG1sbnM6cmRmPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5LzAyLzIyLXJkZi1zeW50YXgtbnMjIj4gPHJkZjpEZXNjcmlwdGlvbiByZGY6YWJvdXQ9IiIgeG1sbnM6eG1wPSJodHRwOi8vbnMuYWRvYmUuY29tL3hhcC8xLjAvIiB4bWxuczp4bXBNTT0iaHR0cDovL25zLmFkb2JlLmNvbS94YXAvMS4wL21tLyIgeG1sbnM6c3RSZWY9Imh0dHA6Ly9ucy5hZG9iZS5jb20veGFwLzEuMC9zVHlwZS9SZXNvdXJjZVJlZiMiIHhtcDpDcmVhdG9yVG9vbD0iQWRvYmUgUGhvdG9zaG9wIENTNiAoV2luZG93cykiIHhtcE1NOkluc3RhbmNlSUQ9InhtcC5paWQ6RkM4QjYyNDVGMTE4MTFFMTlBREZCNDNEM0ExMTk0MUIiIHhtcE1NOkRvY3VtZW50SUQ9InhtcC5kaWQ6RkM4QjYyNDZGMTE4MTFFMTlBREZCNDNEM0ExMTk0MUIiPiA8eG1wTU06RGVyaXZlZEZyb20gc3RSZWY6aW5zdGFuY2VJRD0ieG1wLmlpZDpGQzhCNjI0M0YxMTgxMUUxOUFERkI0M0QzQTExOTQxQiIgc3RSZWY6ZG9jdW1lbnRJRD0ieG1wLmRpZDpGQzhCNjI0NEYxMTgxMUUxOUFERkI0M0QzQTExOTQxQiIvPiA8L3JkZjpEZXNjcmlwdGlvbj4gPC9yZGY6UkRGPiA8L3g6eG1wbWV0YT4gPD94cGFja2V0IGVuZD0iciI/Pkf1yeMAAAJbSURBVHjahFNdTxNBFD0tLULpB91uodVWPmorUIxo0VSiNSExMYYHE33l0Ud/in+C+OSjYgjRGDBRCKJIUkIEWi0WKlja0ul22+5219lJ26gLeiezuXvn7rnnnrlrUFUVms3Mvd2bjIyezRVLBA0zGAzo6jhjm1te+7EU37rFO+w7JlMbtG+ePJ5mOaZmci/nsPl6ONBtw18WDQc9tZq0sp7YjTisXV/NFKRpRvzHpHodDqsF03djzuvDg6vHJWFAprF/Arxe/oins6+YryoqCiUBvNOO+7FrXMjnWc0WyIAOQP0N4Nn8IqqSzPx2swllsYqZl28gK8DDyRvcxKXQvB6gISYpiwgH+jEVi7YAfW4nEqk0PJwDofNejAX7Pae2sPhhHQoF63U5Gai2Bn1epoPWmmaKoug1UBoMrgwHabIVVCx2jdrKFwm67TZ2plldPQGg2cK5HheIUMbaZqKV9In6giDCy3MNYXECgKI2gICxoQAEsQItpNCHWKngMo01arTY/jFIzbutShJuXh1Fm9FImYiM7tTtKOtbO+toN9Nc+fQ5SGUOIVYl7HzPIH2YRZ0y2KZ+sVzBHn2v1mpMGx0DTaR3nzfwfGEJdybGkdo/wEigDyvxLzg4yiESvojZhfd49OAeLJ2degaSLIPOO6vwgiYaaRErTRREEdn8MeJbSVZ5M7nLdNExqFLaQwEfFfACQn1+HBWKSKb3MT4Sgstuh9vVDa+bQ4DORE6o6RlspzMk9TOPfr+fiLJCLFYr3TZSKNcI7+aJwWQmPM+TkqRg49tu65f/JcAAMwMas6WUKd8AAAAASUVORK5CYII=);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-sql {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAh5JREFUeNp8kctrE1EUxr+ZyXMkoa1NBROaSkpTBE23PhZ25cql2y5duvAPUdGFS1FxIRRBXZlFQ9GVdDENIhGJxkDsw2mneZnM83ruNZlOmNoDhzlzz3d/9zv3Sowx8Ch/qlYK2XM3cEJsbH0+qjV/rd6/u6aN18b7RMFT+9aosP/Ex+0ae/puw7j36PlKEMAzctKJ3aGFamMHjV0d+wcGitkMrpWWp6hVIciEk2MAOwbUWjosx0UiFoWqJpGMx5DNzODq5aIPoa82AWBg/lyKLMH1PMp/a9XvLXLzG1cuFlBaWpiKxaIPSLY6CaC93ggQjyiQZRkeQSzLRovGaPciWLt5faSWEBoh6KBvOhiaNga0+Y9pwaFxvu7rfp8F5pWDt+qNMp2IijHGwddWCvN+33/CoAOP5nVdT9SdoQ1JkggiQ6Yvr7V60+9z7akA2gfH9cRF8hO5F5Ve4lQAF9uuK+qFsylkzsQxrcaQm04hdWkR83Mzfp9rQ3fAFzu9Ph6+WMfjl6/pGBdb2jbKmx8QlRjWy5vkyhUZBPgOeGNHN9AbDLGUz6He2hVj3Ll9C8/evsdgaMK0HV8bcmDTU0UUBYXcedR+NLGnH0I3jvDk1Rsy46FP4C/1BtrdntCGHNiOAzWZgEKQ5Qt5lIqLojbaXSQTcRy2OwT4SZqk0IYAOgkVWUE+lxX/zb0DpFNpkTzmZmfFtzewhHYcfwUYAMZmVaZQlLFHAAAAAElFTkSuQmCC);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-tga {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAnxJREFUeNp0U89PE0EU/ra725K22ILRGipb22pMG6JcSEQTbUIwnozxpBcvepeEP0KPogcT/wlNT17kIKbEmChFUYKGVtL0R2gLtNCl3Z1Z3+zSAlonmezOe/O+973vvZEsy4JYnqdPMu6RkSQYQ29JEkB+PZcrslrtPhQl23VZc8/tr9I1yMHg0EA8HrBM04lVFAhoY38fSSDQVN3pfKV8G7KcxZHl6v1xblqU3eLc3p2VFZjr6+gQgwsnhzGTuq6Nhs6kYZqXjwL0GFhEl3U60OfnwWs1GGtrUKNRsKkpeIIBpKIRtI1J7cX7hXRhc/MOhXw5DkCZGG2zXAajzFIoBMvng1ypIKOqmP30GW3OIEcimovzlxRy5RgAFwDEAIODkCcmIMdiQLsNdWwMZdJlg8pzEUt1aBhKq3XinxKYqF9yQbqRIqsMy+0Gyy47bKgUWXSLtDENE5wdtuqQATm50F1VnPbRGeEw8HXZbiV8fsDvI9ldju9vADAyihLEbrWAZhOoVp3z6iqBUiB1A4nEfwCEsbkL/M4TgE5n5jDx+oTEzp1d8m9tC8H6MaAB0imzx0NU/WKUYE+loEyawDBo2ui6TGfT6ANAxrvx87gYCGCxXEKVJvCWFsG3eh1vN/J4OD6Od4UC8o0G3TX7TGLHwI9iEQmvF9X6Fh7F4/iYy+GcLOMSlfEgGsP0qdNOmX0BiGKpVkV1bw/1nW2b/gCpf1PTcI+Y7eg6ps+G4bG4PR99SjAVo9HE4q+fKNE0vl5awuSohjeijbRefVjAtUgEQRK7Yhi9OKn7nKWZxxlSPWl3QwgnaIrW8QMhD542vUbx/W49m7sq4v4IMABOqi3Ej7bAEAAAAABJRU5ErkJggg==);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-tgz {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAnhJREFUeNpsU1trE0EYPbMzSTfdtInFtkkpiaXVWou2FRUEn/so6JugL/oH/Af+B1988if40jcFERQURNBSQdDWlLQN2lsue8neZsZvc7FoOrDszM75znfOmVmmtUYyvry++36yfOeS1qqzDtvH2P76ApPlW3Drb2sHex/uccHWAdbZX30kO2+B3siN3zhTnHuQ66+95i423jzFzOVljBdKOZNHazvVT7e5wF+SZBj9iZJ+3J11mbW2kR8T4LwFli5i4fqTUvnczTUp9RLtDhKgJx0q4dEwWAxrREKICHEsoYYXMXvlcWmquLgmY71yCkG/c0AkARgLMZpnMDMpGNzEYe0dGp6HwvmHpbHC1Wf9MnFCkHQOyYEPzSJwQ2B65Tm5NZG3Fshim6wbMNJn4bpHowMKtIqo2COgR2IcAptwjvcgo6i77igjEmVDqbY8xQJ1VwRULhiBI6+G9Zf3cbTziuzIDkmHSNqECTFgQScEcYuc2NA8TcdYwXD+GkK/TYVN+u72WrIudiAD8o6oAR2RRCmQMjis3CIy1iSpPySCXhFTXeyAgh4BR+JVw8pauLi0Cp4yCX9A90FQhnSBYtnF/k+Q+HYam9itfIZB3QvT8zj8XSW5EhNTs9ivbSLwPUzPLNPJBIMEKnaQYg6aB9+RGR5F5VsNgnNKXMI1NdJGG5WfHzFVLJ7k8c8xUngpVodlDSGbFYj8Y4yMpOG09lHf3yIFPzA3fwHZTAQVtU4JUTeFDrdgDdlI8wAz5Qy2KxswReI7QODZcOr0ZH3q2hIDBI7zq16tuk3FNPxAI4wN+pkoccYoE4YJU5EdUtM4Qst26v26PwIMAKj3P/2YUKgYAAAAAElFTkSuQmCC);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-tiff {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAmRJREFUeNp0UktPE1EU/qYzHWstlrYJNcWUElyUJsaNGh9B0g1Lo0v9Ey78EbrVxBhXuHShm25YGBJRQpAYBDEWpaEPEhksdVpbyjzveO4MfZDCTWbauefc736PIziOA77OPH2yJCcSGdg2uksQAKofFou/7VrtASRpvVNynj13f6XOhjg8HAlMTIQdy/LO+v3uYUPTkAHCTb+cK+0pdyGK6+hbvu4/xiyHbncYAwfR19ZgbG/DoO9LsSgeTd9JXoxfyMG2rvQDdBlwIZauQ5ufh12twioU4E+nYU1NIRCNIDs+Bt28mXzx8VNuZ796j9q/DgAwomwqClilAmF0FE4wCInAlkjO4y+r0JgNX2os6XPYS2q/cQyAcQatFjA0BPH6NYipccAwIGUy2CVJFZInkKlyJAqx3T4/IMGmJkeWIWSz5KgI5pdhb3yDXS5DSCYh8rTID8s0wexeVD0GtMd85KkkefFxUfE47M1NokbJkByEQl6tL+ouAI+MUwbFhnYbaJKc/Sqg0x4H4eDRGDA56fUOABA9/GsCpaIHwr8FOhQ823O5RfW66tUGADhNy3RNRDjcN41HLxdQ8J6jYTsOQLfOJBK4f+s2/uoathoNGKT1MtFeVHZxdWTEZfEq/wMKl3rCJOIzTV6ADs2R5ulYDDNkYjp0DhrF+zCVgkw31+v1UxjQZkNV0SADd2o1MIuc9gmY+/kLxb0/UFoHePd9A1qzeUoKpilx9xcLWzgg+u/zeVfuQqkM9bCN1ysrWKXxdtPgvScwUAm58XZ52W16QyPtifRUzi588GbEi1ztHPsvwAC4uC9qhnsZvwAAAABJRU5ErkJggg==);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-txt {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAeJJREFUeNp8UrtOG1EQPfsyXiyzBguIJSyChZBBEFCKpKHLo6egpErNn8CHgH8gkZIiTSIXLhJAWCgkoMgRMSiRBSK29z4y9+I1d/HCrFb3MTPnnjkzlpQSynY+fP70fGF2gQuByCz6lfdd9Uurfvrrjes6762eb3tzQ69uFJwPsqOPC+MBEmxxphi4tlU5OGmsOzaBWLc+O9oIIVhScidkyGZ8vH62nHtSKlaI4cse6TjAfSaFBBcco0EWqyvzubmpyQrj/FXk75cQaSEMeMXU8xykPA/Hjd/6/LRcyjEpt2i7HAe4A2TeLZWKUOJaVLxj27j813EHGKCXaAJExu/4BOdiAED08riQD2riOrexyRoYc3CvsAbLGAAjZga7vgZG23WMCdBvoxKJc36TRBlMiaa2JByjNqqD8qkYc1pjDK7abey+/YhrWlfKswhpiCR96aEU9o5+QE3g2ovVWDm2Sc22bBQm8vrVpbkS9r+doPr1EOWZaQ0yFoxg2PcREosEAI4uvZhJpzFMP+cSXRbq+043RManez+tNWKMI6GN0g0Z04HFR+NoNC/0yx717efZOSbzY3AcR4Op2AGA5p/W31r9e0vNgSrh9OwCrpeCkqvZuqTybnpRqx/r2CjvvwADAJC/7lzAzQmwAAAAAElFTkSuQmCC);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-wav {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAApFJREFUeNpsU1tPE0EYPXtpKbX0wqUQKVQMFdIXQBNCQBs06KP+B8ODGh+Mf4b/4IsGE54kxhcMBrkp7YOQgBRvSKG73fvsrt8Otoask0xmd+b7zpxzvm8E3/cRjPkniyulW0NFy2JoDkEAguOlpXJ9p3L8MBqVl4O9YHxae8pXuRlcGO7KPLhfTDVUqwUgigJMy4Whm6lEXHjxYf3XnByRN0QB/2KaH7btMlUxoRJAcyqKhdOaht7+DJ49n+2cvTnwynXcsb+kLwJ4rgfmMDDGWqvneXCZS9ND7mov5h9ND85M9y86Dpto5rUkuJ4Py3YDJpy6QGJPayqB+Njf+43XL220t0cwOZkfrNXsBUqZugDA6CbLdAiAwaek1ZU9LmP8Rh6S78GsGxjOp9FdzKJaVZIhBgGASzK21w/wbrnCk8euX+EMAjaaZuPHdwUdHVFYluuGPGCORwwYjg5rqOwccRk+3Ux0IEvntmsNG4ZmUayL/wAwKHUNfZfTKN0ZRaw9Cof8qJ/pMAyHy5KkAMTksSEJtnMenM7EMVMawbejMzJRh67bXEYiIXEAVTW50SEAhzqwfqrBcXx4VOhYm4RsNgHbsJFOyZTsQ1MN+hcohoUlkFiMT+TQFpMwXOjGpXgE+XwGk1N5pFJtKNCequgYGupCRBbCDOp0KBJc4VoP3dyBONW8uydBgBHUThqQKCk3mEZ/LoUG+RBioJO7VarAwEAntjYPiUUW9Hh4b2R7k9j98hN37xWx8fGAt3eIAdVMLn+uUv+b2KReSCZjZJiB9bV9jIz2ofr1BKvvd7G9dRC80lae0HzOt+cWVnrSKDrMJykifwNBpCgE/UAllEXufmDu8Zlffvvm8XSQ90eAAQA0pF7c08o4PAAAAABJRU5ErkJggg==);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-wmv {
	background-image:url("data:image/svg+xml;charset=utf8,%3Csvg id='Layer_2' xmlns='http://www.w3.org/2000/svg' viewBox='0 0 72 100'%3E%3Cstyle/%3E%3ClinearGradient id='SVGID_1_' gradientUnits='userSpaceOnUse' x1='36.2' y1='101' x2='36.2' y2='3.005' gradientTransform='matrix(1 0 0 -1 0 102)'%3E%3Cstop offset='0' stop-color='%23e2cde4'/%3E%3Cstop offset='.17' stop-color='%23e0cae2'/%3E%3Cstop offset='.313' stop-color='%23dbc0dd'/%3E%3Cstop offset='.447' stop-color='%23d2b1d4'/%3E%3Cstop offset='.575' stop-color='%23c79dc7'/%3E%3Cstop offset='.698' stop-color='%23ba84b9'/%3E%3Cstop offset='.819' stop-color='%23ab68a9'/%3E%3Cstop offset='.934' stop-color='%239c4598'/%3E%3Cstop offset='1' stop-color='%23932a8e'/%3E%3C/linearGradient%3E%3Cpath d='M45.2 1l27 26.7V99H.2V1h45z' fill='url(%23SVGID_1_)'/%3E%3Cpath d='M45.2 1l27 26.7V99H.2V1h45z' fill-opacity='0' stroke='%23882383' stroke-width='2'/%3E%3Cpath d='M9.1 91.1L4.7 72.5h3.9l2.8 12.8 3.4-12.8h4.5l3.3 13 2.9-13h3.8l-4.6 18.6h-4L17 77.2l-3.7 13.9H9.1zm22.1 0V72.5h5.7l3.4 12.7 3.4-12.7h5.7v18.6h-3.5V76.4l-3.7 14.7h-3.7l-3.7-14.7v14.7h-3.6zm26.7 0l-6.7-18.6h4.1l4.8 13.8 4.6-13.8h4L62 91.1h-4.1z' fill='%23fff'/%3E%3ClinearGradient id='SVGID_2_' gradientUnits='userSpaceOnUse' x1='18.2' y1='50.023' x2='18.2' y2='50.023' gradientTransform='matrix(1 0 0 -1 0 102)'%3E%3Cstop offset='.005' stop-color='%23963491'/%3E%3Cstop offset='1' stop-color='%2370136b'/%3E%3C/linearGradient%3E%3ClinearGradient id='SVGID_3_' gradientUnits='userSpaceOnUse' x1='11.511' y1='51.716' x2='65.211' y2='51.716' gradientTransform='matrix(1 0 0 -1 0 102)'%3E%3Cstop offset='.005' stop-color='%23963491'/%3E%3Cstop offset='1' stop-color='%2370136b'/%3E%3C/linearGradient%3E%3Cpath d='M64.3 55.5c-1.7-.2-3.4-.3-5.1-.3-7.3-.1-13.3 1.6-18.8 3.7S29.6 63.6 23.3 64c-3.4.2-7.3-.6-8.5-2.4-.8-1.3-.8-3.5-1-5.7-.6-5.7-1.6-11.7-2.4-17.3.8-.9 2.1-1.3 3.4-1.7.4 1.1.2 2.7.6 3.8 7.1.7 13.6-.4 20-1.5 6.3-1.1 12.4-2.2 19.4-2.6 3.4-.2 6.9-.2 10.3 0m-9.9 15.3c.5-.2 1.1-.3 1.9-.2.2-3.7.3-7.3.3-11.2-6.2.2-11.9.9-17 2.2.2 4 .4 7.8.3 12 4-1.1 7.7-2.5 12.6-2.7m2-12.1h1.1c.4-.4.2-1.2.2-1.9-1.5-.6-1.8 1-1.3 1.9zm3.9-.2h1.5V38h-1.3c0 .7-.4.9-.2 1.7zm4 0c.5-.1.8 0 1.1.2.4-.3.2-1.2.2-1.9h-1.3v1.7zm-11.5.3h.9c.4-.3.2-1.2.2-1.9-1.4-.4-1.6 1.2-1.1 1.9zm-4 .4c.7.2.8-.3 1.5-.2v-1.7c-1.5-.4-1.7.6-1.5 1.9zm-3.6-1.1c0 .6-.1 1.4.2 1.7.5.1.5-.4 1.1-.2-.2-.6.5-2-.4-1.9-.1.4-.8.1-.9.4zm-31.5.8c.4-.1 1.1.6 1.3 0-.5 0-.1-.8-.2-1.1-.7.2-1.3.3-1.1 1.1zm28.3-.4c-.3.3.2 1.1 0 1.9.6.2.6-.3 1.1-.2-.2-.6.5-2-.4-1.9-.1.3-.4.2-.7.2zm-3.5 2.8c.5-.1.9-.2 1.3-.4.2-.8-.4-.9-.2-1.7h-.9c-.3.3-.1 1.3-.2 2.1zm26.9-1.8c-2.1-.1-3.3-.2-5.5-.2-.5 3.4 0 7.8-.5 11.2 2.4 0 3.6.1 5.8.3M33.4 41.6c.5.2.1 1.2.2 1.7.5-.1 1.1-.2 1.5-.4.6-1.9-.9-2.4-1.7-1.3zm-4.7.6v1.9c.9.2 1.2-.2 1.9-.2-.1-.7.2-1.7-.2-2.1-.5.2-1.3.1-1.7.4zm-5.3.6c.3.5 0 1.6.4 2.1.7.1.8-.4 1.5-.2-.1-.7-.3-1.2-.2-2.1-.8-.2-.9.3-1.7.2zm-7.5 2H17c.2-.9-.4-1.2-.2-2.1-.4.1-1.2-.3-1.3.2.6.2-.1 1.7.4 1.9zm3.4 1c.1 4.1.9 9.3 1.4 13.7 8 .1 13.1-2.7 19.2-4.5-.5-3.9.1-8.7-.7-12.2-6.2 1.6-12.1 3.2-19.9 3zm.5-.8h1.1c.4-.5-.2-1.2 0-2.1h-1.5c.1.7.1 1.6.4 2.1zm-5.4 7.8c.2 0 .3.2.4.4-.4-.7-.7.5-.2.6.1-.2 0-.4.2-.4.3.5-.8.7-.2.8.7-.5 1.3-1.2 2.4-1.5-.1 1.5.4 2.4.4 3.8-.7.5-1.7.7-1.9 1.7 1.2.7 2.5 1.2 4.2 1.3-.7-4.9-1.1-8.8-1.6-13.7-2.2.3-4-.8-5.1-.9.9.8.6 2.5.8 3.6 0-.2 0-.4.2-.4-.1.7.1 1.7-.2 2.1.7.3.5-.2.4.9m44.6 3.2h1.1c.3-.3.2-1.1.2-1.7h-1.3v1.7zm-4-1.4v1.3c.4.4.7-.2 1.5 0v-1.5c-.6 0-1.2 0-1.5.2zm7.6 1.4h1.3v-1.5h-1.3c.1.5 0 1 0 1.5zm-11-1v1.3h1.1c.3-.3.4-1.7-.2-1.7-.1.4-.8.1-.9.4zm-3.6.4c.1.6-.3 1.7.4 1.7 0-.3.5-.2.9-.2-.2-.5.4-1.8-.4-1.7-.1.3-.6.2-.9.2zm-3.4 1v1.5c.7.2.6-.4 1.3-.2-.2-.5.4-1.8-.4-1.7-.1.3-.8.2-.9.4zM15 57c.7-.5 1.3-1.7.2-2.3-.7.4-.8 1.6-.2 2.3zm26.1-1.3c-.1.7.4.8.2 1.5.9 0 1.2-.6 1.1-1.7-.4-.5-.8.1-1.3.2zm-3 2.7c1 0 1.2-.8 1.1-1.9h-.9c-.3.4-.1 1.3-.2 1.9zm-3.6-.4v1.7c.6-.1 1.3-.2 1.5-.8-.6 0 .3-1.6-.6-1.3 0 .4-.7.1-.9.4zM16 60.8c-.4-.7-.2-2-1.3-1.9.2.7.2 2.7 1.3 1.9zm13.8-.9c.5 0 .1.9.2 1.3.8.1 1.2-.2 1.7-.4v-1.7c-.9-.1-1.6.1-1.9.8zm-4.7.6c0 .8-.1 1.7.4 1.9 0-.5.8-.1 1.1-.2.3-.3-.2-1.1 0-1.9-.7-.2-1 .1-1.5.2zM19 62.3v-1.7c-.5 0-.6-.4-1.3-.2-.1 1.1 0 2.1 1.3 1.9zm2.5.2h1.3c.2-.9-.3-1.1-.2-1.9h-1.3c-.1.9.2 1.2.2 1.9z' fill='url(%23SVGID_3_)'/%3E%3ClinearGradient id='SVGID_4_' gradientUnits='userSpaceOnUse' x1='45.269' y1='74.206' x2='58.769' y2='87.706' gradientTransform='matrix(1 0 0 -1 0 102)'%3E%3Cstop offset='0' stop-color='%23f9eff6'/%3E%3Cstop offset='.378' stop-color='%23f8edf5'/%3E%3Cstop offset='.515' stop-color='%23f3e6f1'/%3E%3Cstop offset='.612' stop-color='%23ecdbeb'/%3E%3Cstop offset='.69' stop-color='%23e3cce2'/%3E%3Cstop offset='.757' stop-color='%23d7b8d7'/%3E%3Cstop offset='.817' stop-color='%23caa1c9'/%3E%3Cstop offset='.871' stop-color='%23bc88bb'/%3E%3Cstop offset='.921' stop-color='%23ae6cab'/%3E%3Cstop offset='.965' stop-color='%239f4d9b'/%3E%3Cstop offset='1' stop-color='%23932a8e'/%3E%3C/linearGradient%3E%3Cpath d='M45.2 1l27 26.7h-27V1z' fill='url(%23SVGID_4_)'/%3E%3Cpath d='M45.2 1l27 26.7h-27V1z' fill-opacity='0' stroke='%23882383' stroke-width='2' stroke-linejoin='bevel'/%3E%3C/svg%3E");
	background-repeat:no-repeat;
	background-size:contain
}

.icon-xls {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAmxJREFUeNpsU0trFEEQ/mamZ3Y2+0zIC2MmITEkUYgERFQErx5E8KTi1b/h79A/4SW3nCNeYggBYZVEMU/y3N3Z7M7OTD/G6lk2ruw20zRdU/XV91VVG0mSQK/3n1a/jky6d6Xs3G8WXS+Pw5N6LXjLLGuna/78oZKerGsYKtrDE16uJGL1L9gEOOcYd2dL1fNwrbL//aXN7J1efPMmkUqEFAk0A0VZNbFEaQCBscIkXj975y3NLq9xye8PBkAniHOFph+j2eC4rsdoB4LsFubGl/Hq8RtvYWpxTQi52o1jvWiGYaRZL0/auDgOkC/Z8BYL2Pqxidp1FZkhoDxpeaXA/Ujuj/4HoOxKKjiOiek7RUShRNQWaNYFQuMafrYCxiw4ozZKfqbYJ0EvRdl1DQyyTs8XCNTA6UELMwvDyLpZWIZNNlNLlQOK2LMJRJ+5AkuZ1S7CFFzJzk56GnUjQWlYkqCoBWFbonEVYcLLA4dNnB624GQsDBWIgfZJEgxkoChzSFWvn4VpQemDm2VwXQsXJwF1h6c+gxlQ5jgSiEUEt0wdIe7tMES+nEG2aCLiJMOIIWIr9e0DEELAMUrwRuchVAyTKimUwO75Jm6VF3Bv7imOaj+xd7UFKVS/BPJF1b/E4tgTrE49J60O5kceoNqowiuuYKa8ghHXA48U9MT2AQgyRvTThE30bQiaSGa4yLMJNFo+Dq/2cHt4CYlwyFf2S6BHwwrMw/avDbR5C1k7h1YQ4KH3Amf+AcZyEbZPv9CItzQD1l9EbtYOjv74v/d3O9RMPTDrsEwGIWN8q2yk7XNYRs9JrRv3V4ABADSGR6eQ0/NQAAAAAElFTkSuQmCC);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-xlsx {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAmlJREFUeNpsU8tqFEEUPVXdPY/ueWZIoiYZiSYKYhJc6EbduHOhgijo3t/wH1z6B0JAhOyMILhxo4kJGk1ASTAxwWF0Mpp5dHc9vFUzYwidaoqmq+8959xzbzGtNcx69PTS26ETmQtS9r4Hy/xv7MW7jV+th5yzVcaYPX/++It9u4NAv+CVR6tBUUTqMJsDcRzjZOZM8W9ZLKx+/XDb4e5/kH5In0lpIYWGUaC0YTZnBCAEKoVR3L36oDo7NbsglZwbqD6iQKOXFMcKUVfBkBAoQhlD5xxMDp/HrSv3q1JgYW3z0x0KXzkCYJaRZljru23aHWTzLiamAyytv0O9UYdf5PArqlppBfMUfu4oALErqZBKcUxMFRCHEp0DgW5Lo4N9NIN1dF0XXsVFOUyPJTzo+WBANDidjp8tgHGG3c0DnJ4uIRf4cOCBaW5KjY8xkZL72xpJ9QcFz5bVqHUJGHZL2YtNmKi06YCyiVFb4s/vEKMTAf1p4edOG6mMi1zR6wEpdUwX+vLDtkCzHoK7ptcM6ayLmGajvtex4PliyoIkFRjmUEASelB2rXQRSfjUCT9PlWpmW21iTGzCAyEkUixPRqXhe2V4zKczbdmybgkpJ0cGOuA6Y2MTCsKoi5HsNK7N3MN+uwYaWbxYfoLLkzdxcew6lrYWaZhm8PHHG3zffp1UwJSHz9vvkU8PodbcQYYYS5lxYkxTkGdVDQdV1Js1qPgYD6JIuIE7gsXVefIhIuM05k7dwMbeMmh87a18ufIMaVYyprrJLgje2Nr+1tzYXANnDnr3zRhHj37Vvy2wpXHtNAd5/wQYAD6WMuT2CwoVAAAAAElFTkSuQmCC);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-xml {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAilJREFUeNqMks1PE0EYxh+g3W2t1G0sEqyISynUFJsSOShNwCamiYZED3LgIkcuxoN/iCZePZiYGD2aGD+i0F5KMChxlVaakAK2ykcAt+WzdLu7zkxo3WZL4pu8mXfmeeY3885ug67roPFh5nvc62m9hjoR+5LMp7MrkYf370qVtco+VtCUFpbj+jGR+JbWn76OyQ8ePwsZATQb8R/hanZgINgj9IqeuBFCw1Kt9OMBnNWCs24XwkG/QKYUEiGjVAPQof/rq0783pShET3ULQo8xz0iS5FaANmrHQH2DoqY+DSLSz6RzecWlnD9ymU47LYjd4O5BXqDTG4FM3NpTEkpdJ5rw0AowLRMbhUfp58gTOaD/UHmNQPI6YmvKWRX1zESHUJ/oBs2nmPa+Mgw0ZIM3tZyGoJwygzQNB2jNyJIZX7iB0lpPoM70UGmPX8zCU+rG8NDVxHwdiC5mKsPUFUN/gvtLLf39sFzVqaN3YrC6TjBauqhXhNA1TQoqloV7Da+pjZq1FsXUCamF29j6LvYhf3iISamZ3Fv9DZevouhRzzPfOG+3hpA9U9UyioOlTJ7pFeTCQS6RGzIebyf+oz5pSzWtmSW1EO9phvQ00slBRt/8qR3DoWdXbiczUiTzd52D+tdLmyTB14mx1rMAKVcRpEATjrsuElee/HXGmnFRyBOGD30C/nEDjNgs7CDpsYmnHG3YPegBCvHs9oYfm8nG9dJa5X4K8AAQzQX4KSN3wcAAAAASUVORK5CYII=);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-yml {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAdxJREFUeNqMUl1rE0EUPbM7m5Y0Zptu21AwWwhYpfSDFh+kvvRd8N0Hf4I/xWdf/Q158F0QoQ+CVsFKaLSQpt/dpmvztTOzzky6cetOpWcZZvbO3MO5514SxzEU3r57/3GpWllM/tP4sL3TarROXuSo/SWJvX71Uu80Cfhlr/T4UdWFAVfdnmsTUtvdP35OUyQKVnJgXDBTcj9icAsTeLax7j/052qM81UjwW1QJXEhMF0qYnN90fdnvdogYmvJPU0/VBApD4hcDrWRcyikfB17srzgW7b9Rh1vEvxDlI4tVytaBSEEtmWh0xsUMwpwnWjqAlcxogiHd1wiQyCu87iI/+sJtf6+NXsgpd7FWCMB50KvkYMGMbLdZgLlfj+K9K4+FnFQ2x7WntIs50AbmiGwLILt+k+EvzvSNIHzdigdJ/AmXQRhiHv5POSwYmG+cqPVo0HqDxj8uTK2vn1Hfa+JmdIkvtZ/4fOPXU3WPDpFeNWVyUKryCiIGMN4zsH98gym3CIcOTwT+XHdXrdQQHAZotE8kBPpSqPNHtBOr48HUmLOcXRJT9dWNMGYJFby91pHOAvaykSaITg+bwefdhrteDRTMSwyrFCgI88E056Hy+4Ah2cXQZL3R4ABALUe7fqXWFN6AAAAAElFTkSuQmCC);
	background-repeat:no-repeat;
	background-size:contain
}

.icon-zip {
	background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAm9JREFUeNpsk0tv00AUhc+MY6dOmgeFJg1FoVVpUWlFC0s2IFF1jxBbhKj4BSxYdscPYcEmQmIDq0gsERIViy4TpD7VFzF1Ho5je2a4thOqNhlp5Mz4zudzzp0wpRTC8fPrk0/TC6+fDtYicLH97T1Kc2vQDcs+rH3eUAxVznn0fn1DRM8E+iOdv5ct3XmZG6yVlNj6solUbgVTt0q5FGtX6vXqC6VklTE+KAO/OODHSIQPRQpsXC+kkEz2ELA0ystv84tLzyucsbWByisAGf+QAS2CCDRRLMJMmxC+i8C4jdLCm/zM7OOKFGptcO6/BTpJ0yeQB0Y+mfKQuZZG0jQgeRbW8Xdomobs9LN8scc+UPHNy4Dwq8IljotIIQEm59/RoSyM1CKkXKZNBm7kIVgyM6wgAnSgRK9vqQfHPiMFDHqyFVsLR9Cm0o4YzoAASrSjCelQfRPb1Vc4qn0EY5L2W9GEaBLcxQgFHpGbkMIDJ69e+wjJ8VXqRgKid0r7ftQdxkRs9SqA2kgAm14SSIQh9uhuLGPMnKJs/5KquL1x0N0RCsizigoDaLqBdHoMiyvrlBsHVx1wphD4BCewoqxGKKDwAgtOy8JufYuk+5golGGaGZwc1sIGoDz3AOPZSVLaHgVwydoJDM1H4DbQODughB3YpOD44HfoHgnu4e7So0uAi0stHLJ3Aud8B9bpHu6vPoSu9TtDl6tUuoFiIYOgu0+158MKmOxomtyD3Qi/3MTR7i8K0EDG1GHO5DE3X4DvNahZlJOwEkOATvdPc2//hx3mXJ5lFJaF8K8bStd0YGfnOJbMGex21x6c+yfAAOlIPDJzr7cLAAAAAElFTkSuQmCC);
	background-repeat:no-repeat;
	background-size:contain
}
"##;
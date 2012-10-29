#!/usr/bin/env python
import re
import email
from email.Utils import parseaddr
from email.Header import decode_header

# email address REGEX matching the RFC 2822 spec
# from perlfaq9
#    my $atom       = qr{[a-zA-Z0-9_!#\$\%&'*+/=?\^`{}~|\-]+};
#    my $dot_atom   = qr{$atom(?:\.$atom)*};
#    my $quoted     = qr{"(?:\\[^\r\n]|[^\\"])*"};
#    my $local      = qr{(?:$dot_atom|$quoted)};
#    my $domain_lit = qr{\[(?:\\\S|[\x21-\x5a\x5e-\x7e])*\]};
#    my $domain     = qr{(?:$dot_atom|$domain_lit)};
#    my $addr_spec  = qr{$local\@$domain};
# 
# Python translation
    
atom_rfc2822=r"[a-zA-Z0-9_!#\$\%&'*+/=?\^`{}~|\-]+"
atom_posfix_restricted=r"[a-zA-Z0-9_#\$&'*+/=?\^`{}~|\-]+" # without '!' and '%'
atom=atom_rfc2822
dot_atom=atom  +  r"(?:\."  +  atom  +  ")*"
quoted=r'"(?:\\[^\r\n]|[^\\"])*"'
local="(?:"  +  dot_atom  +  "|"  +  quoted  +  ")"
domain_lit=r"\[(?:\\\S|[\x21-\x5a\x5e-\x7e])*\]"
domain="(?:"  +  dot_atom  +  "|"  +  domain_lit  +  ")"
addr_spec=local  +  "\@"  +  domain

email_address_re=re.compile('^'+addr_spec+'$')

raw="""MIME-Version: 1.0
Received: by 10.229.233.76 with HTTP; Sat, 2 Jul 2011 04:30:31 -0700 (PDT)
Date: Sat, 2 Jul 2011 13:30:31 +0200
Delivered-To: alain.spineux@gmail.com
Message-ID: <CAAJL_=kPAJZ=fryb21wBOALp8-XOEL-h9j84s3SjpXYQjN3Z3A@mail.gmail.com>
Subject: =?ISO-8859-1?Q?Dr.=20Pointcarr=E9?=
From: Alain Spineux <alain.spineux@gmail.com>
To: =?ISO-8859-1?Q?Dr=2E_Pointcarr=E9?= <alain.spineux@gmail.com>
Content-Type: multipart/alternative; boundary=000e0cd68f223dea3904a714768b

--000e0cd68f223dea3904a714768b
Content-Type: text/plain; charset=ISO-8859-1

-- 
Alain Spineux

--000e0cd68f223dea3904a714768b
Content-Type: text/html; charset=ISO-8859-1



-- 
Alain Spineux


--000e0cd68f223dea3904a714768b--
"""

def getmailheader(header_text, default="ascii"):
    """Decode header_text if needed"""
    try:
        headers=decode_header(header_text)
    except email.Errors.HeaderParseError:
        # This already append in email.base64mime.decode()
        # instead return a sanitized ascii string 
        return header_text.encode('ascii', 'replace').decode('ascii')
    else:
        for i, (text, charset) in enumerate(headers):
            try:
                headers[i]=unicode(text, charset or default, errors='replace')
            except LookupError:
                # if the charset is unknown, force default 
                headers[i]=unicode(text, default, errors='replace')
        return u"".join(headers)

def getmailaddresses(msg, name):
    """retrieve From:, To: and Cc: addresses"""
    addrs=email.utils.getaddresses(msg.get_all(name, []))
    for i, (name, addr) in enumerate(addrs):
        if not name and addr:
            # only one string! Is it the address or is it the name ?
            # use the same for both and see later
            name=addr
            
        try:
            # address must be ascii only
            addr=addr.encode('ascii')
        except UnicodeError:
            addr=''
        else:
            # address must match adress regex
            if not email_address_re.match(addr):
                addr=''
        addrs[i]=(getmailheader(name), addr)
    return addrs

msg=email.message_from_string(raw)
subject=getmailheader(msg.get('Subject', ''))
from_=getmailaddresses(msg, 'from')
from_=('', '') if not from_ else from_[0]
tos=getmailaddresses(msg, 'to')
    
print 'Subject: %r' % subject
print 'From: %r' % (from_, )
print 'To: %r' % (tos, )

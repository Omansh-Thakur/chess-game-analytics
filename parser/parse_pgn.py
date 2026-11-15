import sys, csv, re

COMMENT = re.compile('[{]([^}]*)[}]')
LB, RB = re.escape(chr(91)), re.escape(chr(93))
CLK = re.compile(LB + '%clk ([0-9]+):([0-9]+):([0-9.]+)' + RB)
EVAL = re.compile(LB + '%eval (#?-?[0-9.]+)' + RB)
NUMS = re.compile('[0-9]+[.]+')
RESULTS = ('1-0', '0-1', '1/2-1/2', '*')
SPEEDS = ('UltraBullet', 'Bullet', 'Blitz', 'Rapid', 'Classical', 'Correspondence')

GAME_COLS = ['game_id', 'date', 'time', 'speed', 'time_control', 'white_elo', 'black_elo',
             'result', 'termination', 'eco', 'opening', 'plies', 'has_clock', 'has_eval']
MOVE_COLS = ['game_id', 'ply', 'color', 'clock_sec', 'eval_cp', 'loss_cp', 'mover_elo', 'speed']


def eval_cp(text):
    if text.startswith('#'):
        return 1000 if not text.startswith('#-') else -1000
    return max(-1000, min(1000, int(float(text) * 100)))


def handle(tags, moves, gw, mw):
    comments = COMMENT.findall(moves)
    clean = NUMS.sub(' ', COMMENT.sub(' ', moves))
    tokens = [t for t in clean.split() if t not in RESULTS and not t.startswith('$')]
    plies = len(tokens)
    event = tags.get('Event', '')
    speed = next((s for s in SPEEDS if s.lower() in event.lower()), 'Unknown')
    gid = tags.get('Site', '').rsplit('/', 1)[-1]
    has_clock = any(CLK.search(c) for c in comments)
    has_eval = any(EVAL.search(c) for c in comments)

    gw.writerow([gid, tags.get('UTCDate', ''), tags.get('UTCTime', ''), speed,
                 tags.get('TimeControl', ''), tags.get('WhiteElo', ''), tags.get('BlackElo', ''),
                 tags.get('Result', ''), tags.get('Termination', ''), tags.get('ECO', ''),
                 tags.get('Opening', ''), plies, int(has_clock), int(has_eval)])

    if not (has_clock and has_eval and len(comments) == plies and plies > 1):
        return

    prev = 20  # starting evaluation in centipawns
    for i, c in enumerate(comments):
        m_clk, m_eval = CLK.search(c), EVAL.search(c)
        if not (m_clk and m_eval):
            return
        cp = eval_cp(m_eval.group(1))
        white = (i % 2 == 0)
        loss = (prev - cp) if white else (cp - prev)  # positive = mover got worse
        clk = int(m_clk.group(1)) * 3600 + int(m_clk.group(2)) * 60 + int(float(m_clk.group(3)))
        elo = tags.get('WhiteElo' if white else 'BlackElo', '')
        if i > 0:
            mw.writerow([gid, i + 1, 'white' if white else 'black', clk, cp, loss, elo, speed])
        prev = cp


def main(games_path, moves_path):
    sys.stdin.reconfigure(encoding='utf-8', errors='replace')
    tags = {}
    n = 0
    with open(games_path, 'w', newline='') as gf, open(moves_path, 'w', newline='') as mf:
        gw = csv.writer(gf, lineterminator=chr(10))
        mw = csv.writer(mf, lineterminator=chr(10))
        gw.writerow(GAME_COLS)
        mw.writerow(MOVE_COLS)
        for raw in sys.stdin:
            line = raw.strip()
            if not line:
                continue
            if line.startswith('[') and line.endswith(']'):
                key, _, rest = line[1:-1].partition(' ')
                if key == 'Event':
                    tags = {}
                tags[key] = rest[1:-1] if rest.startswith(chr(34)) else rest
                continue
            if tags:
                handle(tags, line, gw, mw)
                n += 1
                if n % 100000 == 0:
                    print(n, 'games parsed', file=sys.stderr)
            tags = {}
    print('done:', n, 'games', file=sys.stderr)


if __name__ == '__main__':
    if len(sys.argv) != 3:
        sys.exit('usage: zstdcat file.pgn.zst | python3 parse_pgn.py games.csv moves.csv')
    main(sys.argv[1], sys.argv[2])


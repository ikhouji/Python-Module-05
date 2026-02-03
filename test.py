#!/usr/bin/env python3
"""
maze_curses_player.py

Generates a maze using recursive backtracker, draws it with curses using
Unicode box-drawing characters, and shows a BFS solver animation.

Changes:
- Player character is now a "real" glyph (☺).
- The path the player takes is colored blue.

Controls:
  n - new maze
  s - solve (animate shortest path)
  r - regenerate with random seed
  q - quit

Optional command-line args:
  python maze_curses_player.py [cols] [rows]
  (cols/rows refer to cell counts, not terminal characters)
"""

import curses
import random
import time
import sys
from collections import deque
import locale

# ensure Unicode output works
locale.setlocale(locale.LC_ALL, "")

# Player glyph (single-character). Using U+263A WHITE SMILING FACE ("☺").
# If your font/terminal doesn't display it well, replace with another single char.
PLAYER_GLYPH = "0"
PATH_GLYPH = "·"  # middle dot used to mark the path

# Directions: (dx, dy), and the corresponding wall between cells
DIRS = {
    "N": (0, -1),
    "S": (0, 1),
    "W": (-1, 0),
    "E": (1, 0),
}
OPPOSITE = {"N": "S", "S": "N", "W": "E", "E": "W"}


class Maze:
    def __init__(self, cell_cols, cell_rows, seed=None):
        self.cell_cols = cell_cols
        self.cell_rows = cell_rows
        self.seed = seed
        if seed is not None:
            random.seed(seed)
        self.cells = {
            (x, y): set() for y in range(cell_rows) for x in range(cell_cols)
        }
        self._generate()

    def _generate(self):
        stack = []
        start = (0, 0)
        visited = {start}
        stack.append(start)

        while stack:
            x, y = stack[-1]
            neighbors = []
            for d, (dx, dy) in DIRS.items():
                nx, ny = x + dx, y + dy
                if 0 <= nx < self.cell_cols and 0 <= ny < self.cell_rows:
                    if (nx, ny) not in visited:
                        neighbors.append((d, nx, ny))
            if neighbors:
                d, nx, ny = random.choice(neighbors)
                self.cells[(x, y)].add(d)
                self.cells[(nx, ny)].add(OPPOSITE[d])
                visited.add((nx, ny))
                stack.append((nx, ny))
            else:
                stack.pop()

    def to_grid(self):
        """
        Convert the cell-based maze to a character grid. Passages are ' '.
        Grid dims: rows = cell_rows*2+1, cols = cell_cols*2+1
        """
        rows = self.cell_rows * 2 + 1
        cols = self.cell_cols * 2 + 1
        grid = [["█" for _ in range(cols)] for _ in range(rows)]  # default block for walls

        for y in range(self.cell_rows):
            for x in range(self.cell_cols):
                gx, gy = x * 2 + 1, y * 2 + 1
                grid[gy][gx] = " "  # passage cell
                dirs = self.cells[(x, y)]
                if "E" in dirs:
                    grid[gy][gx + 1] = " "  # open to east
                if "S" in dirs:
                    grid[gy + 1][gx] = " "  # open to south

        return grid

    def start_end_positions(self):
        start_cell = (0, 0)
        end_cell = (self.cell_cols - 1, self.cell_rows - 1)
        start = (start_cell[0] * 2 + 1, start_cell[1] * 2 + 1)
        end = (end_cell[0] * 2 + 1, end_cell[1] * 2 + 1)
        return start, end


def bfs_shortest_path(grid, start, goal):
    """BFS in grid (list of lists). start and goal are (x, y). Returns list of (x,y)."""
    cols = len(grid[0])
    rows = len(grid)
    q = deque()
    q.append(start)
    prev = {start: None}
    while q:
        x, y = q.popleft()
        if (x, y) == goal:
            break
        for dx, dy in [(1, 0), (-1, 0), (0, 1), (0, -1)]:
            nx, ny = x + dx, y + dy
            if 0 <= nx < cols and 0 <= ny < rows:
                if grid[ny][nx] == " " or (nx, ny) == goal:
                    if (nx, ny) not in prev:
                        prev[(nx, ny)] = (x, y)
                        q.append((nx, ny))
    if goal not in prev:
        return []
    path = []
    cur = goal
    while cur is not None:
        path.append(cur)
        cur = prev[cur]
    path.reverse()
    return path


# mapping of neighbor walls to box-drawing char
# bits: N=1, E=2, S=4, W=8
BOX_MAP = {
    0: "█",
    1: "│",
    2: "─",
    3: "└",
    4: "│",
    5: "│",
    6: "┌",
    7: "├",
    8: "─",
    9: "┘",
    10: "─",
    11: "┴",
    12: "┐",
    13: "┤",
    14: "┬",
    15: "┼",
}


def wall_box_char(grid, x, y):
    """
    For a wall cell at (x,y) determine which neighbors are also walls
    and return the appropriate box-drawing character.
    """
    rows = len(grid)
    cols = len(grid[0])

    def is_wall(nx, ny):
        if not (0 <= nx < cols and 0 <= ny < rows):
            return False
        return grid[ny][nx] != " "

    bits = 0
    if is_wall(x, y - 1):
        bits |= 1  # N
    if is_wall(x + 1, y):
        bits |= 2  # E
    if is_wall(x, y + 1):
        bits |= 4  # S
    if is_wall(x - 1, y):
        bits |= 8  # W

    return BOX_MAP.get(bits, "█")


def build_visual_grid(grid, start, end, pathset=None, player=None):
    """
    Return a visual grid (list of strings) where wall cells are replaced
    with box-drawing chars and passages are spaces. pathset and player
    override characters for visualization.
    """
    rows = len(grid)
    cols = len(grid[0])
    visual = [[" " for _ in range(cols)] for _ in range(rows)]

    for y in range(rows):
        for x in range(cols):
            if grid[y][x] == " ":
                visual[y][x] = " "
            else:
                visual[y][x] = wall_box_char(grid, x, y)

    # overlay path and player and start/end
    if pathset:
        for (px, py) in pathset:
            # don't overwrite start/end or player
            if (px, py) == start or (px, py) == end or (px, py) == player:
                continue
            visual[py][px] = PATH_GLYPH  # middle dot for path

    if player:
        px, py = player
        visual[py][px] = PLAYER_GLYPH

    sx, sy = start
    ex, ey = end
    visual[sy][sx] = "S"
    visual[ey][ex] = "E"

    # convert rows to strings
    return ["".join(visual[r]) for r in range(rows)]


def draw_in_curses(stdscr, grid, start, end, path=None, player=None, info=None):
    """
    Draw grid to stdscr using box characters and colored path/player.
    path: iterable of (x,y) coordinates to draw as PATH_GLYPH
    player: (x,y) coordinate to highlight current player pos (PLAYER_GLYPH)
    info: list of strings to write below the maze
    """
    stdscr.erase()
    rows = len(grid)
    cols = len(grid[0])

    # Colors if available
    curses.start_color()
    curses.use_default_colors()
    try:
        curses.init_pair(1, curses.COLOR_WHITE, -1)   # walls/box-drawing
        curses.init_pair(2, curses.COLOR_BLACK, -1)   # passage/default
        curses.init_pair(3, curses.COLOR_BLUE, -1)    # path (blue)  <-- user's request
        curses.init_pair(4, curses.COLOR_RED, -1)     # start/end
        curses.init_pair(5, curses.COLOR_YELLOW, -1)  # player glyph color
    except curses.error:
        # terminal might not support colors
        pass

    pathset = set(path) if path else set()
    visual_lines = build_visual_grid(grid, start, end, pathset=pathset, player=player)

    for y, line in enumerate(visual_lines):
        for x, ch in enumerate(line):
            # choose attribute based on character
            attr = curses.A_NORMAL
            try:
                if ch == PATH_GLYPH:
                    attr = curses.color_pair(3) | curses.A_BOLD
                elif ch == PLAYER_GLYPH:
                    attr = curses.color_pair(5) | curses.A_BOLD
                elif ch == "S" or ch == "E":
                    attr = curses.color_pair(4) | curses.A_BOLD
                elif ch == " ":
                    attr = curses.color_pair(2)
                else:
                    # walls / box characters
                    attr = curses.color_pair(1) | curses.A_BOLD
            except curses.error:
                # fallback if colors not supported
                if ch == PATH_GLYPH:
                    attr = curses.A_BOLD
                elif ch == PLAYER_GLYPH:
                    attr = curses.A_BOLD
                elif ch in ("S", "E"):
                    attr = curses.A_BOLD
                elif ch == " ":
                    attr = curses.A_NORMAL
                else:
                    attr = curses.A_BOLD

            try:
                stdscr.addch(y, x, ch, attr)
            except curses.error:
                # some terminals may not support wide chars or might be out of bounds
                # fallback: attempt addstr for the slice
                try:
                    stdscr.addstr(y, x, ch)
                except curses.error:
                    pass

    # draw info below maze
    if info:
        for i, line in enumerate(info):
            try:
                stdscr.addstr(rows + i, 0, line[: curses.COLS - 1])
            except curses.error:
                pass

    stdscr.refresh()


def main_curses(stdscr, cell_cols, cell_rows, seed=None):
    curses.curs_set(0)
    stdscr.nodelay(False)
    stdscr.keypad(True)

    def make_and_draw(randomize_seed=False):
        nonlocal seed
        if randomize_seed:
            seed = random.randrange(1 << 30)
        maze = Maze(cell_cols, cell_rows, seed)
        grid = maze.to_grid()
        start, end = maze.start_end_positions()
        info = [
            "Controls: n=new maze | s=solve | r=randomize seed | q=quit",
            f"Cells: {cell_cols}x{cell_rows}  Seed: {seed}",
            "Press s to start solver animation.",
        ]
        draw_in_curses(stdscr, grid, start, end, path=None, player=None, info=info)
        return maze, grid, start, end

    maze, grid, start, end = make_and_draw()

    while True:
        try:
            ch = stdscr.getch()
        except KeyboardInterrupt:
            break
        if ch in (ord("q"), ord("Q")):
            break
        elif ch in (ord("n"), ord("N")):
            maze, grid, start, end = make_and_draw(randomize_seed=False)
        elif ch in (ord("r"), ord("R")):
            maze, grid, start, end = make_and_draw(randomize_seed=True)
        elif ch in (ord("s"), ord("S")):
            path = bfs_shortest_path(grid, start, end)
            if not path:
                info = ["No path found."]
                draw_in_curses(stdscr, grid, start, end, path=None, player=None, info=info)
                continue
            info = ["Solving: animating path... (press q to abort)"]
            # animate exploring the final path: blue path + moving player glyph
            for i, pos in enumerate(path):
                draw_in_curses(stdscr, grid, start, end, path=path[: i + 1], player=pos, info=info)
                # allow user to abort quickly
                stdscr.timeout(20)
                c = stdscr.getch()
                if c in (ord("q"), ord("Q")):
                    stdscr.timeout(-1)
                    return
                time.sleep(0.02)
            stdscr.timeout(-1)
            info = ["Solved. Press n to generate new maze, r to randomize seed, q to quit."]
            draw_in_curses(stdscr, grid, start, end, path=path, player=end, info=info)
        else:
            # ignore other keys
            pass


def auto_size_cells_from_terminal():
    try:
        rows, cols = map(int, curses.initscr().getmaxyx())
        curses.endwin()
    except Exception:
        cols, rows = 80, 24
    max_cols = max(3, (cols - 1) // 2)
    max_rows = max(3, (rows - 4) // 2)
    cell_cols = min(40, max(4, max_cols))
    cell_rows = min(20, max(4, max_rows))
    return cell_cols, cell_rows


if __name__ == "__main__":
    if len(sys.argv) >= 3:
        try:
            cell_cols = int(sys.argv[1])
            cell_rows = int(sys.argv[2])
        except ValueError:
            print("Usage: python maze_curses_player.py [cell_cols] [cell_rows]")
            sys.exit(1)
    else:
        cell_cols, cell_rows = auto_size_cells_from_terminal()

    seed = None

    try:
        curses.wrapper(main_curses, cell_cols, cell_rows, seed)
    except curses.error as e:
        print("Curses error:", e)
        print("If you're on Windows, try: pip install windows-curses")
        sys.exit(1)
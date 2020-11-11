<?php
function getCombination(array $game): string
{
    sort($game);
    $repeats = array_count_values($game);
    $countUnique = count($repeats);
    $maxRepeat = max($repeats);
    $sequence = [];
    foreach ($game as $i => $item) {
        $sequence[$i] = 0;
        for ($min = $item, $j = $i; $j < count($game); $j++) {
            if ($min === $game[$j]) {
                continue;
            }
            if ($game[$j] === $min + 1) {
                $min = $game[$j];
                $sequence[$i]++;
            } else {
                break;
            }
        }
    }
    $maxSequence = max($sequence);

    if ($maxRepeat === 5) {
        return 'покер';
    }

    if ($maxRepeat === 4) {
        return 'Каре';
    }
    if ($maxRepeat === 3 && $countUnique === 2) {
        return 'фул хаус';
    }
    if ($countUnique === 5 && $maxSequence === 5) {
        return 'большой стрит';
    }
    if ($countUnique === 4 && $maxSequence === 4) {
        return 'малый стрит';
    }
    if ($maxRepeat === 3) {
        return 'сэт';
    }
    if ($maxRepeat === 2 && $countUnique === 3) {
        return 'две пары';
    }
    if ($maxRepeat === 2) {
        return 'пара';
    }
    return 'шанс';
}

$game = array_map(static function () {
    return rand(1, 6);
}, array_fill(0, 5, null));


var_dump($game);
var_dump(getCombination($game));



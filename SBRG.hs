-- ghci -fbreak-on-error
-- :set -fbreak-on-error
-- :set args 1 XYZ [4] [1,1,1] 1
-- :trace main

{-# LANGUAGE TupleSections, BangPatterns, MagicHash, MultiParamTypeClasses, FlexibleInstances #-} -- OverloadedLists
-- :set -XTupleSections

{-# OPTIONS -Wall -fno-warn-unused-binds -fno-warn-unused-imports -O2 -optc-O2 -optc-march=native -optc-mfpmath=sse #-}
-- -rtsopts -prof -fprof-auto        -ddump-simpl -threaded
-- +RTS -xc -sstderr -p              -N4

import GHC.Prim (reallyUnsafePtrEquality#)
import GHC.Exts (sortWith, the)

import Control.Monad
import qualified Control.Monad.ST as ST
import Data.Bits
import Data.Coerce
import Data.Either
import Data.Function
import Data.Foldable
import Data.List
import Data.Maybe
import Data.Monoid
import Data.Ord
import Data.Tuple
import Debug.Trace
import Numeric.IEEE (nan,infinity)
import System.Environment
import System.CPUTime
import System.Exit (exitSuccess, exitFailure)

--import Control.Monad.Trans.State.Strict (State)
import qualified Control.Monad.Trans.State.Strict as State

import Data.IntSet (IntSet)
import qualified Data.IntSet as IntSet

import Data.Set (Set)
import qualified Data.Set as Set

import Data.IntMap.Strict (IntMap)
import qualified Data.IntMap.Strict as IntMap hiding (fromList, insert, delete, adjust, adjustWithKey, update, updateWithKey) -- use alter instead

import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map hiding (fromList, insert, delete, adjust, adjustWithKey, update, updateWithKey) -- use alter instead

import qualified System.Random as Random

-- debug

debug :: Bool
debug = True

check :: Bool -> String -> a -> a
check b s x = not debug || b ? x $ error s

debugShow :: Show a => a -> a
debugShow x = trace ('\n': show x) x

-- generic

-- http://stackoverflow.com/a/9650222
-- http://lpaste.net/1364
ptrEquality :: a -> a -> Bool
ptrEquality !x !y =
  case reallyUnsafePtrEquality# x y of
       1# -> True
       _  -> False

if' :: Bool -> a -> a -> a
if' True  x _ = x
if' False _ y = y

infixr 1 ?
(?) :: Bool -> a -> a -> a
(?) = if'

boole :: Num a => Bool -> a
boole False = 0
boole True  = 1

infixr 9 .*
{-# INLINE (.*) #-}
(.*) :: (c -> c') -> (a -> b -> c) -> a -> b -> c'
f .* g = \x y -> f $ g x y

infixr 9 .**
{-# INLINE (.**) #-}
(.**) :: (d -> d') -> (a -> b -> c -> d) -> a -> b -> c -> d'
f .** g = \x y z -> f $ g x y z

-- fromLeft :: Either a b -> a
-- fromLeft (Left x) = x
-- fromLeft _        = error "fromLeft"

-- fromRight :: Either a b -> b
-- fromRight (Right y) = y
-- fromRight _         = error "fromRight"

infixl 7 //
(//) :: Integral a => a -> a -> a
x // y | (q,0) <- divMod x y = q
       | otherwise           = error "//"

forceF :: Foldable t => t a -> t a
forceF x = foldl' (flip seq) x x

-- force2 :: (a,b) -> (a,b)
-- force2 xy@(x,y) = x `seq` y `seq` xy

-- force3 :: (a,b,c) -> (a,b,c)
-- force3 xyz@(x,y,z) = x `seq` y `seq` z `seq` xyz

fst3 :: (a,b,c) -> a
fst3 (x,_,_) = x

snd3 :: (a,b,c) -> b
snd3 (_,y,_) = y

thd3 :: (a,b,c) -> c
thd3 (_,_,z) = z

mapPair :: (a -> a', b -> b') -> (a,b) -> (a',b')
mapPair (f,g) (x,y) = (f x, g y)

mapBoth :: (a -> b) -> (a,a) -> (b,b)
mapBoth f = mapPair (f,f)

mapFst :: (a -> a') -> (a,b) -> (a',b)
mapFst f = mapPair (f, id)

mapSnd :: (b -> b') -> (a,b) -> (a,b')
mapSnd f = mapPair (id, f)

mapThd3 :: (c -> c') -> (a,b,c) -> (a,b,c')
mapThd3 f (x,y,z) = (x,y,f z)

uncurry3 :: (a -> b -> c -> d) -> (a, b, c) -> d
uncurry3 f (x,y,z) = f x y z

mapHead :: (a -> a) -> [a] -> [a]
mapHead f (x:xs) = f x : xs
mapHead _ _      = error "mapHead"

foldl'_ :: Foldable t => (a -> b -> b) -> t a -> b -> b
foldl'_ = flip . foldl' . flip

partitions :: Int -> [a] -> [[a]]
partitions _ [] = []
partitions n xs = uncurry (:) $ mapSnd (partitions n) $ splitAt n xs

newtype NestedFold t1 t2 a = NestedFold { getNestedFold :: t1 (t2 a) }

instance (Foldable t1, Foldable t2) => Foldable (NestedFold t1 t2) where
  foldr  f x = foldr  (flip $ foldr  f) x . getNestedFold
  foldl' f x = foldl' (       foldl' f) x . getNestedFold

type NestedFold2 t1 t2 t3    a = NestedFold (NestedFold t1 t2) t3 a
type NestedFold3 t1 t2 t3 t4 a = NestedFold (NestedFold (NestedFold t1 t2) t3) t4 a

nestedFold :: t1 (t2 a) -> NestedFold t1 t2 a
nestedFold  = NestedFold

nestedFold2 :: t1 (t2 (t3 a)) -> NestedFold2 t1 t2 t3 a
nestedFold2 = NestedFold . nestedFold

nestedFold3 :: t1 (t2 (t3 (t4 a))) -> NestedFold3 t1 t2 t3 t4 a
nestedFold3 = NestedFold . nestedFold2

getArgs7 :: (Read t1, Read t2, Read t3, Read t4, Read t5, Read t6, Read t7) => [String] -> IO (t1,t2,t3,t4,t5,t6,t7)
getArgs7 defaults = do
  args <- getArgs
  let n = 7
      [x1,x2,x3,x4,x5,x6,x7] = args ++ drop (length defaults + length args - n) defaults
  return (read x1, read x2, read x3, read x4, read x5, read x6, read x7)

infixl 1 `applyIf`
applyIf :: (a -> a) -> (a -> Bool) -> a -> a
applyIf f q x = if' (q x) f id $ x

nest :: Int -> (a -> a) -> a -> a
nest 0 _ = id
nest n f = nest (n-1) f . f

justIf :: (a -> Bool) -> a -> Maybe a
justIf q x = q x ? Just x $ Nothing

-- minimumOn :: Ord b => (a -> b) -> [a] -> a
-- minimumOn f xs = snd $ minimumBy (comparing fst) $ map (\x -> (f x, x)) xs

-- maximumOn :: Ord b => (a -> b) -> [a] -> a
-- maximumOn f xs = snd $ maximumBy (comparing fst) $ map (\x -> (f x, x)) xs

intUnion :: [Int] -> [Int]
intUnion = IntSet.toList . IntSet.fromList

unions :: Eq a => [[a]] -> [a]
unions = foldl' union []

mergeUsing :: (a -> a -> a) -> a -> [a] -> a
mergeUsing f x_ xs_ = merge $ x_:xs_
  where merge [x] = x
        merge  xs = merge $ mergePairs xs
        
        mergePairs (x:y:xs) = f x y : mergePairs xs
        mergePairs xs = xs

mergeSortedBy :: (a -> a -> Ordering) -> [[a]] -> [a]
mergeSortedBy cmp = mergeUsing mergePair []
  where mergePair xs@(x:xs') ys@(y:ys') | x `cmp` y == LT = x : mergePair xs' ys
                                        | otherwise       = y : mergePair xs  ys'
        mergePair [] ys = ys
        mergePair xs [] = xs

-- same as mergeSortedBy except uses f to combine equal elements
mergeUnionsBy :: (a -> a -> Ordering) -> (a -> a -> a) -> [[a]] -> [a]
mergeUnionsBy cmp f = mergeUsing mergePair []
  where mergePair xs@(x:xs') ys@(y:ys') = case x `cmp` y of
                                               LT ->   x   : mergePair xs' ys
                                               EQ -> f x y : mergePair xs' ys'
                                               GT ->     y : mergePair xs  ys'
        mergePair [] ys = ys
        mergePair xs [] = xs

sq :: Num a => a -> a
sq x = x*x

isPow2 :: Int -> Bool
isPow2 x = popCount x == 1

mean :: Floating f => [f] -> f
mean xs = sum xs / (fromIntegral $ length xs)

rms :: Floating f => [f] -> f
rms = sqrt . mean . map sq

rss :: Floating f => [f] -> f
rss = sqrt . sum . map sq

meanError :: Floating f => [f] -> (f, f)
meanError xs  = (mean0, sqrt $ (sum $ map (sq . (mean0-)) xs) / ((n-1)*n) )
  where n     = fromIntegral $ length xs
        mean0 = sum xs / n

epsilon, epsilon_2 :: Fractional f => f
epsilon   = 2 ^^ (-52::Int)
epsilon_2 = 2 ^^ (-26::Int)

-- Nothing if x+y is tiny compared to x and y
infixl 6 +?
(+?) :: F -> F -> Maybe F
x +? y = x_y*x_y < epsilon*(x*x + y*y) ? Nothing $ Just x_y
  where x_y = x + y

-- input must be sorted
modDifferences :: Int -> [Int] -> [Int]
modDifferences l is = (head is + l - last is) : zipWith (-) (tail is) is

-- LogFloat
-- https://hackage.haskell.org/package/logfloat-0.13.3.1/docs/Data-Number-LogFloat.html#v:product

data LogFloat = LogFloat Bool Double

-- note: don't define toDouble'LF
-- only showsPrec can use it safely

fromDouble'LF :: Double -> LogFloat
fromDouble'LF x = LogFloat (not $ x<0 || isNegativeZero x) (log $ abs x)

-- preserves sign of base
pow'LF :: LogFloat -> Double -> LogFloat
pow'LF (LogFloat s y) x = LogFloat s $ y*x
-- pow'LF _ _ = error "pow'LF"

log'LF :: LogFloat -> Double
log'LF (LogFloat True y) = y
log'LF _                 = error "log'LF"

isNaN'LF :: LogFloat -> Bool
isNaN'LF (LogFloat _ y) = isNaN y
  
instance Show LogFloat where
  showsPrec n (LogFloat s y) str
    | abs y > 600 && not (isInfinite y || isNaN y)
      = if' s "" "-" ++ (abs y > recip epsilon
                        ? "10^" ++ shows (y/ln10) str
                        $ shows (exp $ ln10*y') ('e' : shows (e::Integer) str) )
    | otherwise = showsPrec n yDouble str
    where (e,y')  = properFraction $ y / ln10
          ln10    = log 10
          yDouble = s ? exp y $ -exp y

instance Read LogFloat where
  readsPrec = map (mapFst fromDouble'LF) .* readsPrec

instance Eq LogFloat where
  (==) = (==EQ) .* compare

instance Ord LogFloat where
  compare (LogFloat s1 y1) (LogFloat s2 y2)
    |    y1 == -infinity
      && y2 == -infinity   = EQ
    | isNaN y1 || isNaN y2 = error "LogFloat compare NaN" -- GT -- http://stackoverflow.com/a/6399798
    |     s1 && not s2     = GT
    | not s1 &&     s2     = LT
    |     s1 &&     s2     = compare y1 y2
    | otherwise            = compare y2 y1

foreign import ccall unsafe "math.h log1p"
    log1p :: Double -> Double -- log(x+1)

instance Num LogFloat where
  z1@(LogFloat s1 y1) + z2@(LogFloat s2 y2)
    | dy == 0 && (s1/=s2) = LogFloat True (-infinity)
    | dy >= 0             = LogFloat s1 $ s1==s2 || dy>1 ? y1 + log1p (if' (s1==s2) id negate $ exp $ -dy)
                                                         $ (y1+y2)/2 + log(2*sinh(dy/2))
    | dy <  0             = z2 + z1
    | isNaN y1            = z1
    | isNaN y2 || s1==s2  = z2
    | y1 < 0              = s1 ? z1 $ z2
    | otherwise           = LogFloat True nan
      where dy = y1 - y2
  (LogFloat s1 y1) * (LogFloat s2 y2) = LogFloat (not $ xor s1 s2) (y1 + y2)
  abs (LogFloat _ y) = LogFloat True y
  signum z@(LogFloat s y)    = (isInfinite y && y < 0) || isNaN y ? z $ LogFloat s 0
  fromInteger = fromDouble'LF . fromInteger
  negate (LogFloat s y) = LogFloat (not s) y

check'LF :: Bool
check'LF = and [isNaN z_ == isNaN'LF z' && (isNaN z_ || (z-eps <= z' && z' <= z+eps && (1/z_>0) == (1/z'>0) && compare x_ y_ == compare x y))
           | x_ <- xs,
             y_ <- xs,
             let [x,y,z] = map fromDouble'LF [x_,y_,z_]
                 z_      = x_ + y_
                 z'      = x  + y
                 eps = 1e-10]
  where xs = [-infinity,-2,-1,-0,0,1,2,infinity,nan]

instance Fractional LogFloat where
  fromRational = fromDouble'LF . fromRational
  recip (LogFloat s y) = LogFloat s (-y)

instance Floating LogFloat where
  sqrt (LogFloat True y) = LogFloat True (y/2)
  sqrt _                 = error "LogFloat sqrt"
  pi    = error "LogFloat pi"
  exp   = error "LogFloat exp"
  log   = error "LogFloat log"
  sin   = undefined
  cos   = undefined
  asin  = undefined
  acos  = undefined
  atan  = undefined
  sinh  = undefined
  cosh  = undefined
  asinh = undefined
  acosh = undefined
  atanh = undefined

-- rankZ2

rankZ2 :: [[Bool]] -> Int
rankZ2 = go 0 . map (foldl' (\x b -> 2*x + boole b) 0)
  where
    go :: Int -> [Integer] -> Int
    go  n []         = n
    go  n (0:rows)   = go n rows
    go !n (row:rows) = go (n+1) $ map (xor row `applyIf` flip testBit j) rows
      where
        j = head $ filter (testBit row) [0..]

-- AddHash

infixl 5 +#
class AddHash a b where
  (+#) :: a -> b -> a

instance AddHash a b => AddHash a [b] where
  (+#) = foldl' (+#)

instance AddHash a b => AddHash a (Maybe b) where
  (+#) = foldl' (+#)

instance AddHash MapGT (Sigma,F) where
  m +# (g,c) = Map.alter (maybe (Just c) (+?c)) g m

instance AddHash MapGT MapGT where
  (+#) = Map.mergeWithKey (\_ x y -> x +? y) id id

instance AddHash Ham SigmaTerm where
  (+#) = flip insert'Ham

instance AddHash Ham MapGT where
  (+#) = Map.foldlWithKey' $ \ham g c -> ham +# (g,c)

instance AddHash MapAbsSigmas SigmaTerm where
  cg +# (g,c) = Map.insertWith Set.union (AbsF c) (Set.singleton g) cg

instance AddHash MapAbsSigmas MapAbsSigmas where
  (+#) = map_union set_union
    where map_union f x y = Map.unionWith f x y
          set_union   x y = Set.union       x y

-- AbsF

newtype AbsF = AbsF {absF :: F}
  deriving (Eq, Show)

instance Ord AbsF where
  AbsF x `compare` AbsF y = compare (abs y) (abs x)

-- Sigma

data Sigma = Sigma {
  ik'G   :: !(IntMap Int),
  iD'G   :: !Int,
  hash'G ::  Word } -- TODO use a hash that can be calculated easily by multSigma

sigma :: Int -> IntMap Int -> Sigma
sigma iD ik  = Sigma ik iD $ calc_hash'G ik iD

calc_hash'G :: IntMap Int -> Int -> Word
calc_hash'G ik iD = mult*fromIntegral iD + IntMap.foldrWithKey' (\i k hash' -> hash' + (fromIntegral k) * bit (mod (2*i) n)) 0 ik
  where mult = 11400714819323198549 -- NextPrime[2^64 2/(1 + Sqrt@5)]
        n    = finiteBitSize (undefined::Word)

set_iD'G :: Int -> Sigma -> Sigma
set_iD'G iD g = g { iD'G = iD, hash'G = calc_hash'G (ik'G g) iD }

instance Eq Sigma where
  Sigma g1 iD1 hash1 == Sigma g2 iD2 hash2 = hash1==hash2 && iD1==iD2 && (ptrEquality g1 g2 || g1==g2)

instance Ord Sigma where
  Sigma g1 iD1 hash1 `compare` Sigma g2 iD2 hash2 =
      compare hash1 hash2 <> compare iD1 iD2 <> (ptrEquality g1 g2 ? EQ $ compare' g1 g2)
    where compare' x y = compare x y

instance Show Sigma where
  showsPrec n (Sigma g _ _) = showsPrec n $ IntMap.toList g

type SigmaTerm = (Sigma, F)

check'Sigma :: Sigma -> Bool
check'Sigma g = all (\k -> 1<=k && k<=3) $ IntMap.elems $ ik'G g

toList'GT :: SigmaTerm -> ([(Int,Int)],F)
toList'GT = mapFst $ IntMap.toList . ik'G

fromList'GT :: Int -> ([(Int,Int)],F) -> SigmaTerm
fromList'GT iD = mapFst $ sigma iD . IntMap.fromListWith (error "fromList'GT")

show'GTs :: [SigmaTerm] -> String
show'GTs = if' prettyPrint (concat . intersperse " + " . map (\(g,c) -> show c ++ " σ" ++ show g)) show

sort'GT :: [(a,F)] -> [(a,F)]
sort'GT = sortWith (AbsF . snd)

scale'GT :: F -> SigmaTerm -> SigmaTerm
scale'GT x (g,c) = (g,x*c)

length'Sigma :: Int -> Sigma -> Int -- TODO d>1
length'Sigma l = (l-) . maximum . modDifferences l . IntMap.keys . ik'G

acommQ :: Sigma -> Sigma -> Bool
acommQ g1 g2 = foldl_ xor False $ (intersectionWith' (/=) `on` ik'G) g1 g2
  where foldl_ f x y = IntMap.foldl' f x y
        intersectionWith' f x y = IntMap.intersectionWith f x y

-- acommQ = acommQ_ `on` ik'G
--   where acommQ_ g1 g2 =
--           case splitRoot1 g1 of
--                [l1,r1] -> case splitRoot2 g2 of
--                                [l2,r2] -> acommQ_ l1 l2 `xor` acommQ_ r1 r2 `xor` acommQ' l1 r2 `xor` acommQ' l2 r1
--                                [v2]    -> f v2 g1
--                                []      -> False
--                                _       -> error "acommQ 2"
--                [v1]    -> f v1 g2
--                []      -> False
--                _       -> error "acommQ 1"
--         f :: IntMap Int -> IntMap Int -> Bool
--         f v g = let [(vk,vx)] = IntMap.toList v
--                 in maybe False (/=vx) $ IntMap.lookup vk g
--         acommQ' l r = (fst $ IntMap.findMax l) >= (fst $ IntMap.findMin r) && acommQ_ l r
--         splitRoot1 x = IntMap.splitRoot x
--         splitRoot2 x = IntMap.splitRoot x

-- the first returned Int is the phase in units of pi/2
multSigma1 :: Int -> Int -> (Int,Int)
multSigma1 1 1 = ( 0,0)
multSigma1 2 2 = ( 0,0)
multSigma1 3 3 = ( 0,0)
multSigma1 1 2 = ( 1,3)
multSigma1 2 1 = (-1,3)
multSigma1 2 3 = ( 1,1)
multSigma1 3 2 = (-1,1)
multSigma1 3 1 = ( 1,2)
multSigma1 1 3 = (-1,2)
multSigma1 k 0 = ( 0,k)
multSigma1 0 k = ( 0,k)
multSigma1 _ _ = error "multSigma1"

multSigma :: Sigma -> Sigma -> (Int,Sigma)
multSigma (Sigma g1 iD1 _) (Sigma g2 iD2 _) = (n, g')
  where n  = IntMap.foldl' (+) 0 $ intersectionWith' (fst .* multSigma1) g1 g2 -- TODO implement IntMap.foldIntersectionWith
        g' = sigma iD $ IntMap.mergeWithKey (\_ k1 k2 -> justIf (/=0) $ snd $ multSigma1 k1 k2) id id g1 g2
        iD | iD2==0 || iD1==iD2 = iD1
           | iD1==0             = iD2
           | otherwise          = error "multSigma"
        intersectionWith' f x y = IntMap.intersectionWith f x y
--         g' = sigma $ filter' (/=0) $ unionWith' (snd .* multSigma1) g1 g2
--         unionWith' f x y = IntMap.unionWith f x y
--         filter' f x = IntMap.filter f x

-- i [a,b]/2
icomm :: Sigma -> Sigma -> Maybe SigmaTerm
icomm g1 g2 = case mod n 4 of
                   0 -> Nothing
                   2 -> Nothing
                   1 -> Just (g,-1)
                   3 -> Just (g, 1)
                   _ -> error "icomm"
  where (n,g) = multSigma g1 g2

icomm'GT :: SigmaTerm -> SigmaTerm -> Maybe SigmaTerm
icomm'GT (g1,c1) (g2,c2) = scale'GT (c1*c2) <$> icomm g1 g2

-- {a,b}/2
acomm :: Sigma -> Sigma -> Maybe SigmaTerm
acomm g1 g2 = case mod n 4 of
                   0 -> Just (g, 1)
                   2 -> Just (g,-1)
                   1 -> Nothing
                   3 -> Nothing
                   _ -> error "acomm"
  where (n,g) = multSigma g1 g2

-- c4 g4 g = R^dg g R where R = (1 + i g4)/sqrt(2)
c4 :: Sigma -> Sigma -> Maybe SigmaTerm
c4 g4 g = icomm g g4

-- TODO very slow
c4s :: F -> [Sigma] -> SigmaTerm -> SigmaTerm
c4s dir g4s gT = foldl' (\gT'@(!g',!c') g4 -> maybe gT' (scale'GT $ dir*c') $ c4 g4 g') gT g4s

-- TODO maybe cache this
localQ'Sigma :: Int -> Sigma -> Bool
localQ'Sigma n = \g -> IntMap.size (ik'G g) < sqrt_n
  where sqrt_n = round $ sqrt $ (fromIntegral n :: Double)

-- MapGT

type MapGT = Map Sigma F

toLists'MapGT :: MapGT -> [([(Int,Int)],F)]
toLists'MapGT = map toList'GT . Map.toList

fromList'MapGT :: [SigmaTerm] -> MapGT
fromList'MapGT = Map.fromListWith (+)

-- MapAbsSigmas

type MapAbsSigmas = Map AbsF (Set Sigma)

delete'MAS :: SigmaTerm -> MapAbsSigmas -> MapAbsSigmas
delete'MAS (g,c) = Map.alter (justIf (not . Set.null) . Set.delete g . fromJust) (AbsF c)

toList'MAS :: MapAbsSigmas -> [SigmaTerm]
toList'MAS gs_ = [(g,c) | (AbsF c,gs) <- Map.toAscList gs_,
                          g <- Set.toList gs]

toList'MASs :: [MapAbsSigmas] -> [SigmaTerm]
toList'MASs gs_ = [ (g',c) | (AbsF c,gs) <- mergeUnionsBy (comparing fst) (\(c,s) (_,s') -> (c, Set.union s s')) $ map Map.toAscList gs_,
                             g' <- Set.toList gs ]

-- Ham

-- MapAbsSigmas is used so that acommCandidates is ordered by coefficient so that max_wolff_terms cuts is more efficient
data Ham = Ham {
  gc'Ham    :: !MapGT,                 -- Hamiltonian:  sigma matrix  -> coefficient
  lcgs'Ham  :: !MapAbsSigmas,          -- |coefficient| ->     local sigma matrices
  nlcgs'Ham :: !MapAbsSigmas,          -- |coefficient| -> non-local sigma matrices
  icgs'Ham  :: !(IntMap MapAbsSigmas), -- local sigmas: site index -> |coefficient| -> local sigma matrices
  ls'Ham    :: ![Int] }                -- system lengths

n'Ham :: Ham -> Int
n'Ham = product . ls'Ham

instance Show Ham where
  show = show'GTs . Map.toList . gc'Ham

-- instance Show Ham where
--   showsPrec _ ham s = "fromLists'Ham " ++ show (ls'Ham ham) ++ " " ++ showsPrec 10 (toLists'Ham ham) s

-- check'Ham :: Ham -> Bool
-- check'Ham ham = ham == (fromLists'Ham (ls'Ham ham) $ toLists'Ham ham) && all check'Sigma (Map.keys $ gc'Ham ham)

zero'Ham :: [Int] -> Ham
zero'Ham ls = Ham Map.empty Map.empty Map.empty IntMap.empty ls

toList'Ham :: Ham -> [SigmaTerm]
toList'Ham = Map.toList . gc'Ham

toLists'Ham :: Ham -> [([(Int,Int)],F)]
toLists'Ham = toLists'MapGT . gc'Ham

toDescList'Ham :: Ham -> [SigmaTerm]
toDescList'Ham ham = toList'MASs [lcgs'Ham ham, nlcgs'Ham ham]

fromList'Ham :: [Int] -> [SigmaTerm] -> Ham
fromList'Ham ls = (zero'Ham ls +#)

fromLists'Ham :: [Int] -> Int -> [([(Int,Int)],F)] -> Ham
fromLists'Ham ls iD = fromList'Ham ls . map (fromList'GT iD)

null'Ham :: Ham -> Bool
null'Ham = Map.null . gc'Ham

insert'Ham :: SigmaTerm -> Ham -> Ham
insert'Ham gT@(g,c) ham@(Ham gc lcgs nlcgs icgs ls)
-- | c == 0       = ham -- this line would break stab'RG
  | isNothing c_ =
    Ham (Map.insertWith (error "insert'Ham") g c gc)
        (localQ ?  lcgs' $  lcgs )
        (localQ ? nlcgs  $ nlcgs')
        (localQ ?  icgs' $  icgs )
                     ls
  |  otherwise = maybe id (insert'Ham . (g,)) (fromJust c_ +? c) $ delete'Ham g ham
  where n      = n'Ham ham
        c_     = Map.lookup g gc
        localQ = localQ'Sigma n g
        lcgs'  = lcgs +# gT
        nlcgs' = nlcgs +# gT
        icgs'  = let cgs_  = Map.singleton (AbsF c) $ Set.singleton g
                 in          IntMap.mergeWithKey (\_ gc0 _ -> Just $ gc0 +# gT) id (IntMap.map $ const cgs_) icgs $ ik'G g

delete'Ham :: Sigma -> Ham -> Ham
delete'Ham = fst .* deleteLookup'Ham

deleteLookup'Ham :: Sigma -> Ham -> (Ham, F)
deleteLookup'Ham g ham@(Ham gc lcgs nlcgs icgs ls) =
  (Ham gc'
       (localQ ?  lcgs' $  lcgs )
       (localQ ? nlcgs  $ nlcgs')
       (localQ ?  icgs' $  icgs )
       ls, c)
  where n             = n'Ham ham
        localQ        = localQ'Sigma n g
        (Just c, gc') = Map.updateLookupWithKey (\_ _ -> Nothing) g gc
        gT            = (g,c)
        lcgs'         = delete'MAS gT lcgs
        nlcgs'        = delete'MAS gT nlcgs
        icgs'         = IntMap.differenceWith (\cgs _ -> justIf (not . Map.null) $ delete'MAS gT cgs) icgs $ ik'G g

deleteSigmas'Ham :: Foldable t => t Sigma -> Ham -> Ham
deleteSigmas'Ham = foldl'_ delete'Ham

acommCandidates :: Sigma -> Ham -> [SigmaTerm]
acommCandidates = acommCandidates_sorted

acommCandidates_sorted :: Sigma -> Ham -> [SigmaTerm]
acommCandidates_sorted g ham = toList'MASs $ nlcgs'Ham ham : IntMap.elems localSigmas
  where localSigmas = IntMap.intersection (icgs'Ham ham) $ ik'G g

nearbySigmas :: Int -> Ham -> MapAbsSigmas
nearbySigmas i ham = nlcgs'Ham ham +# (IntMap.lookup i $ icgs'Ham ham)

icomm'Ham :: SigmaTerm -> Ham -> [SigmaTerm]
icomm'Ham (gT@(g,_)) ham = catMaybes $ map (icomm'GT gT) $ acommCandidates g ham

-- c4'Ham :: F -> Ham -> Sigma -> Ham
-- c4'Ham dir ham g4 = union'Ham ham' $ fromList'MapGT $ Map.elems $ Map.intersectionWith (\(g,c) c' -> (g,dir*c*c')) ggT gc0'
--   where -- map from old Sigma to new Sigma with a sign
--         ggT :: Map Sigma SigmaTerm
--         ggT = Map.mapMaybe id $ Map.fromSet (c4 g4) $ nearbySigmas ham g4
--         -- ham w/o old Sigmas and old Sigmas with coefficients
--         (ham', gc0') = difference'Ham ham $ Map.keysSet ggT

c4'Ham :: F -> Sigma -> Ham -> Ham
c4'Ham dir g4 ham = uncurry (+#) $ foldl' delSigma (ham,[]) $ map fst $ acommCandidates g4 ham
  where delSigma (!ham0,!gTs) g = case c4 g4 g of
                                       Nothing      -> (ham0,gTs)
                                       Just (g',c') -> mapSnd (\c -> (g',dir*c*c'):gTs) $ deleteLookup'Ham g ham0

-- TODO one could transpose (ie apply all c4 to one Sigma at a time instead of one c4 to all Sigma) this function by calculating the locality of the g4s
c4s'Ham :: F -> [Sigma] -> Ham -> Ham
c4s'Ham dir = foldl'_ $ c4'Ham dir

c4_wolff'Ham :: Maybe Int -> F -> G4_H0G -> Ham -> Ham
c4_wolff'Ham = c4_wolff_'Ham False

-- Schrieffer-Wolff: H -> S^dg H S
--                      = H - (2h_3^2)^-1 [H, H_0 Sigma]
--                      = H + ( h_3^2)^-1 icomm H (icomm H_0 Sigma)
--                      = H +             icomm H _H0G
-- note: # of terms cut by max_wolff_terms is affected by iD'G
-- onlyDiag note: only local sigmas are guaranteed to be diagonal -- TODO this doesn't seem to be true
c4_wolff_'Ham :: Bool -> Maybe Int -> F -> G4_H0G -> Ham -> Ham
c4_wolff_'Ham True     _               dir  _ _ | dir /= 1       = error "c4_wolff_'Ham"
c4_wolff_'Ham _        _               dir (Left    g4     ) ham = c4'Ham dir g4 ham
c4_wolff_'Ham onlyDiag max_wolff_terms dir (Right (_H0G,i3)) ham = deleteSigmas'Ham dels ham
                                                                 +# maybe id take max_wolff_terms new
  where dels = not onlyDiag ? [] $ fromMaybe [] $ filter ((/=3) . (IntMap.! i3) . ik'G) . toList . NestedFold <$> IntMap.lookup i3 (icgs'Ham ham)
        new = mergeSortedBy (comparing $ AbsF . snd)
              [catMaybes [scale'GT dir <$> icomm'GT gT gT'
                         | gT <- acommCandidates_sorted (fst gT') ham,
                           not onlyDiag || IntMap.findWithDefault 0 i3 (ik'G $ fst gT) /= 3 ]
              | gT' <- _H0G] -- TODO check sort?

-- TODO onlyDiag makes no guarantees!
c4s_wolff_'Ham :: Bool -> Maybe Int -> F -> [G4_H0G] -> Ham -> Ham
c4s_wolff_'Ham onlyDiag max_wolff_terms dir g4_H0Gs ham_ =
    if' onlyDiag diagFilter id $ foldl'_ (c4_wolff_'Ham onlyDiag max_wolff_terms dir) g4_H0Gs ham_
  where diagFilter ham = ham -- foldl' (\ham' g -> all (==3) (ik'G g) ? ham' $ fst $ deleteLookup'Ham g ham') ham
                       -- $ NestedFold $ nlcgs'Ham ham -- TODO profile this

c4s_wolff'Ham :: Maybe Int -> F -> [G4_H0G] -> Ham -> Ham
c4s_wolff'Ham = c4s_wolff_'Ham False

--

type G4_H0G = Either Sigma ([SigmaTerm],Int) -- Either g4 (icomm H_0 Sigma/h_3^2, i3)

data RG = RG {
  ham0'RG            :: !Ham,              -- original Ham in old basis
  c4_ham0'RG         ::  Ham,              -- original ham in new basis
  ham'RG             :: !Ham,
  diag'RG            :: !(Maybe Ham),
  unusedIs'RG        :: !IntSet,
  g4_H0Gs'RG         :: ![G4_H0G],
  offdiag_errors'RG  :: ![F],
  wolff2_errors'RG   :: ![F],
  trash'RG           :: !(Maybe [[SigmaTerm]]),
  stab0'RG           :: ![SigmaTerm],      -- stabilizers in new basis
  stab1'RG           :: ![SigmaTerm],      -- stabilizers in current basis
  stab'RG            ::  Ham,              -- stabilizers in old basis
  max_rg_terms'RG    :: !(Maybe Int),
  max_wolff_terms'RG :: !(Maybe Int)}

force'RG :: RG -> RG
force'RG = seq' diag'RG . seq' trash'RG
  where seq' f rg = forceF (f rg) `seq` rg

ls'RG :: RG -> [Int]
ls'RG = ls'Ham . ham'RG

n'RG :: RG -> Int
n'RG = n'Ham . ham'RG

g4s'RG :: RG -> [Sigma]
g4s'RG = lefts . g4_H0Gs'RG

i3s'RG :: RG -> [Int]
i3s'RG = map snd . rights . g4_H0Gs'RG

wolff_errors'RG :: RG -> [F]
wolff_errors'RG = map (rss . map snd . fst) . rights . g4_H0Gs'RG

init'RG :: Ham -> RG
init'RG ham = rg
  where rg  = RG ham ham ham (Just $ zero'Ham $ ls'Ham ham) (IntSet.fromList [0..(n'Ham ham - 1)]) [] [] [] (Just []) [] [] (stabilizers rg) Nothing Nothing

-- g ~ sigma matrix, _G ~ Sigma, i ~ site index, h ~ energy coefficient
rgStep :: RG -> RG
rgStep rg@(RG _ c4_ham0 ham1 diag unusedIs g4_H0Gs offdiag_errors _ trash stab0 stab1 _ max_rg_terms _)
  | IntSet.null unusedIs = rg
  | otherwise            = force'RG rg'
  where
    _G1           :: [(SigmaTerm,SigmaTerm)] -- [(icomm g3' gT/h3', -gT)] where gT is in Sigma
    _G2, _G3, _G4 :: [SigmaTerm]
    g3        = fromMaybe g3__ g3_
      where g3_  = (fst<$>) $ listToMaybe $ toDescList'Ham ham1
            g3__ = (!!1) $ sortOn (IntMap.size . ik'G . fst . c4s (-1) (g4s'RG rg) . (,0)) [sigma 0 $ IntMap.fromSet (const k) unusedIs | k <- [1..3]]
    h3        = fromMaybe 0 $ Map.lookup g3 $ gc'Ham ham1
    unusedIs_ = IntMap.fromSet (const ()) unusedIs
  --i3        = snd $ the $ until (null . drop 1) (\is -> let is' = filter (even . fst) is in map (mapFst (flip div 2)) $ null is' ? is $ is') $ map (\i -> (i,i))
    i3        = fst $ maximumBy (comparing snd) $ (\is -> zip is $ modDifferences (n'Ham ham1) is) -- TODO 1d only
              $ IntMap.keys $ IntMap.intersection (ik'G g3) unusedIs_
    unusedIs' = IntSet.delete i3 unusedIs
    g3'       = sigma 0 $ IntMap.singleton i3 3
  --g3'       = sigma 0 $ IntMap.insertWith (error "g3'") i3 3 $ IntMap.difference (ik'G g3) unusedIs_
    g4s_      = find_G4s g3 g3'
    c4_ham0'  = c4s'Ham 1 g4s_ c4_ham0
    g4_H0Gs'  = Right (sort'GT $ map fst _G1, i3) : map Left (reverse g4s_)
    
                    -- apply c4 transformation
    ham2             = c4s'Ham 1 g4s_ ham1
                    -- get g3' coefficient
    h3'              = fromMaybe 0 $ Map.lookup g3' $ gc'Ham ham2
                    -- find sigma matrices that overlap with g3', split into anticommuting commutators (_G1) and diagonal matrices (diag')
    (diag',_G1)      = mapFst (filter isDiag) $ partitionEithers
                     $ map (\gT -> maybe (Left gT) (Right . (,scale'GT (-1) gT)) $ icomm'GT (g3',recip h3') gT)
                     $ toList'MAS $ nearbySigmas i3 ham2              -- the minus is because we apply icomm twice
                    -- remove anticommuting matrices from ham
    ham3             = flip deleteSigmas'Ham ham2 $ map (fst . snd) _G1
                    -- [Sigma, Delta]
    _G_Delta         = [(icomm'Ham (g,1) ham3', c) | (g,c) <- map fst _G1]
                       where ham3' = delete'Ham g3' ham3
                    -- calc offdiag and wolff2 error
    offdiag_error    = (/abs h3') $ maximum $ (0:) $ map (abs . snd) $ concat $ map fst _G_Delta
    wolff2_error     = (rss $ map snd $ Map.toList $ fromList'MapGT $ concat $ map (\(gs,c) -> map (scale'GT c) gs) _G_Delta)
                     / (rss $ map (snd . snd) _G1)
--     offdiag_error    = maximum $ 0:[ abs $ snd gT'
--                                    | g   <- map (fst . snd) _G1,
--                                      gT' <- icomm'Ham (g,1/h3') ham3']
--                        where ham3' = delete'Ham g3' ham3
                    -- remove diagonal matrices from ham
    ham4             = flip deleteSigmas'Ham ham3 $ map fst diag'
                    -- distribute _G1
    _G2              = catMaybes [ icomm'GT gL gR
                                 | gLRs@((gL,_):_) <- init $ tails _G1,
                                   gR <- mapHead (scale'GT 0.5) $ map snd gLRs ]
                    -- calc .5/h3'^2 H_0 [Sigma,Delta]
                    --   = -.5 icomm (icomm g3' Sigma/h3') Delta
--     _G2b             = concat --fst $ cut_terms False max_rg_terms $ mergeSortedBy (comparing $ AbsF . snd)
--                        [ catMaybes [ icomm'GT gL gR
--                                    | gR <- acommCandidates_sorted (fst gL) ham ]
--                        | gL  <- map (\((g,h),_) -> (set_iD'G 1 g, -0.5*h)) _G1,
--                          ham <- [ham4, fromJust diag2] ]
                    -- extract diagonal terms
    (diag'',_G3)    = partition isDiag $ h3'==0 ? [] $ Map.toList $ fromList'MapGT _G2
                    -- keep only max_rg_terms terms
    (_G4, trash')   = cut_terms True max_rg_terms _G3
    
    cut_terms :: Bool -> Maybe Int -> [SigmaTerm] -> ([SigmaTerm],[SigmaTerm])
    cut_terms sortQ = maybe (,[]) (\max_terms -> splitAt max_terms . if' sortQ sort'GT id)
    
    rg' = rg {c4_ham0'RG        = c4_ham0',
              ham'RG            = ham4 +# _G4,
              diag'RG           = (+# [diag',diag'']) <$> diag,
              unusedIs'RG       = unusedIs',
              g4_H0Gs'RG        = g4_H0Gs' ++ g4_H0Gs,
              offdiag_errors'RG = offdiag_error : offdiag_errors,
            --wolff2_errors'RG  = wolff2_error  : wolff2_errors,
              trash'RG          = (trash':) <$> trash,
              stab0'RG          = (g3', h3'):stab0,
              stab1'RG          = (g3 , h3 ):stab1,
              stab'RG           = stabilizers rg'}
    
    isDiag :: SigmaTerm -> Bool
    isDiag (g,_) = iD'G g == 0 && isDiag' g
      where isDiag' = not . any (flip IntSet.member unusedIs') . IntMap.keys . ik'G -- TODO null . IntMap.intersection might be faster
    
    find_G4s :: Sigma -> Sigma -> [Sigma]
    find_G4s g0 g1 = maybe (g0 == g1 ? [] $ find_G4s g_ g1 ++ find_G4s g0 g_) (replicate 1 . fst) $ icomm g0 g1
      where g_ = sigma 0 $ IntMap.singleton i3 1
--       where (i0s,i1s) = mapBoth (IntSet.intersection unusedIs . IntMap.keysSet . ik'G) (g0,g1)
--             i01s      = IntSet.intersection i0s i1s
--             is        = map IntSet.findMin $ IntSet.null i01s ? [i0s,i1s] $ [i01s]
--             g_        = sigma 0 $ IntMap.fromListWith (error "find_G4s") $ [(i, head $ [1,2,3] \\ map (IntMap.findWithDefault 0 i . ik'G) [g0,g1]) | i <- is]

stabilizers :: RG -> Ham
stabilizers rg = c4s'Ham (-1) (g4s'RG rg) $ fromList'Ham (ls'RG rg) $ stab0'RG rg

runRG :: RG -> RG
runRG = until (IntSet.null . unusedIs'RG) rgStep

-- return: 1 == RMS of <g>^2 over all eigenstates
-- Ham: the stabilizer Ham
rmsQ :: Ham -> Sigma -> Bool
rmsQ stab g0 = not $ any (acommQ g0) $ map fst $ acommCandidates g0 stab

-- anderson_corr :: Int -> [Int] -> [Sigma] -> RG -> [(Int,[(Double,Double)])]
-- anderson_corr = anderson_corr' 0

anderson_corr :: Int -> [Int] -> [(Sigma,Sigma)] -> Ham -> [(Int,[(Double,Double)])]
anderson_corr  z xs ggs stab = [(x, [anderson_corr_ stab $ anderson_corr_gs_ z x gg $ ls'Ham stab | gg <- ggs]) | x <- xs]

anderson_corr' :: Int -> [Int] -> [(Sigma,Sigma)] -> RG  -> [(Int,[(F,F)])]
anderson_corr' z xs ggs rg   = [(x, [anderson_corr'_ rg  $ anderson_corr_gs_ z x gg $ ls'RG rg | gg <- ggs]) | x <- xs]

anderson_corr_gs_ :: Int -> Int -> (Sigma,Sigma) -> [Int] -> [(Int, IntMap Int)]
anderson_corr_gs_ z x0 (g0,g0') ls | isJust gg = [(i, g_ i) | i <- [0,z..n-1]]
                                   | otherwise = []
  where n  = lY      * head ls
        lY = product $ tail ls
        gg :: Maybe Sigma -- g(0) g(x0)
        gg | x0 == 0 && g0==g0' = Just g0
           | otherwise          = (fst<$>) $ acomm g0 $ sigma 0 $ IntMap.mapKeys (flip mod n . (+ x0*lY)) $ ik'G g0'
        g_ :: Int -> IntMap Int -- g(i) g(i+x0)
        g_ i = IntMap.mapKeys (i_xs ls . zipWith (+) (xs_i ls i) . xs_i ls) $ ik'G $ fromJust gg

anderson_corr_ ::              Ham -> [(Int, IntMap Int)] -> (Double,Double)
anderson_corr_ _    [] = (0,0)
anderson_corr_ stab gs = meanError $ map (boole . rmsQ stab . sigma 0 . snd) gs

anderson_corr'_ :: RG -> [(Int, IntMap Int)] -> (F,F)
anderson_corr'_ _  [] = (0,0)
anderson_corr'_ rg gs = meanError $ IntMap.elems $ IntMap.fromListWith (+) $ ([(i+1,0) | (i,_) <- gs]++) $ map (\(g,c) -> (iD'G g, c*c)) $ wolf_gs
  where wolf_gs = filter (all (==3) . ik'G . fst) $ Map.toList $ gc'Ham
                $ c4s_wolff_'Ham True (max_wolff_terms'RG rg) 1 (reverse $ g4_H0Gs'RG rg)
                $ fromList'Ham (ls'RG rg) $ map (\(i,g) -> (sigma (i+1) g,1)) gs

ee_slow :: IntSet -> Ham -> Double
ee_slow is stab = (0.5*) $ fromIntegral $ rankZ2 $ acommMat $
    [sigma 0 g'
    | g <- map ik'G $ Map.keys $ gc'Ham stab,
      let g' = IntMap.intersection g $ IntMap.fromSet (const ()) is,
      not (IntMap.null g') && ((/=) `on` IntMap.size) g g']
  where acommMat gs = [[acommQ g g' | g <- gs] | g' <- gs]

ee1d_slow :: [Int] -> Ham -> [(Int,Double,Double)]
ee1d_slow l0s = map (\(l0,_,ee,er) -> (l0,ee,er)) . ee1d_slow' l0s [0]

ee1d_slow' :: [Int] -> [Int] -> Ham -> [(Int,Int,Double,Double)]
ee1d_slow' l0s x0s stab =
    [uncurry (l0,x0,,) $ meanError [ee_slow (is l0 x `IntSet.union` is l0 (x+x0)) stab | x <- [0..lx-1]]
    | l0 <- l0s, x0 <- x0s]
  where n       = n'Ham stab
        lx      =           head $ ls'Ham stab
        lY      = product $ tail $ ls'Ham stab
        modn i  = mod i n
        is l0 x = IntSet.fromList $ map modn [x*lY..(x+l0)*lY-1]

-- entanglement entropy: arguments: list of region sizes and stabilizer Ham
ee1d :: [Int] -> [Int] -> Ham -> [(Int,Int,Double,Double)]
ee1d l0s x0s stab = map (\(l0,x0,ees) -> uncurry (l0,x0,,) $ meanError ees) $ ee1d_ l0s x0s stab

ee1d_ :: [Int] -> [Int] -> Ham -> [(Int,Int,[Double])]
ee1d_ l0s x0s stab = [(l0, x0, [regionEE l0 x0 x | x <- [0..lx-1]]) | l0 <- l0s, x0 <- x0s]
  where
    n  = n'Ham stab
    lx =           head $ ls'Ham stab
    lY = product $ tail $ ls'Ham stab
    -- entanglement entropy of the region [i..i+l-1]
    regionEE :: Int -> Int -> Int -> Double
    regionEE l0 x0 x = (0.5*) $ fromIntegral $ rankZ2 $ acommMat $ union' regionStabs
      where regionStabs = [sigma 0 g'
                          | g <- map ik'G $ localStabs' ++ nonlocalStabs,
                            let g' = (IntMap.union `on` selRegion g . (*lY)) x (modx$x+x0),
                            not (IntMap.null g') && ((/=) `on` IntMap.size) g g']
            localStabs' = unions $ map (IntMap.findWithDefault [] & flip $ cutStabs) $ nub $ map modx [x,x+l0,x+x0,x+x0+l0]
                       -- TODO unions -> Set.unions and NestedFold
            selRegion :: IntMap Int -> Int -> IntMap Int
            selRegion g i = (if i+l<n then                fst . IntMap.split        (i+l)
                                      else IntMap.union $ fst $ IntMap.split (modn $ i+l) g)
                          $ snd $ IntMap.split (i-1) g
            union'     = Set.toList . Set.fromList
            l          = l0*lY
    modx x = mod x lx
    modn i = mod i n
    -- this function ends up being O(N^2) on acommQ if the system is critical
    acommMat :: [Sigma] -> [[Bool]]
    acommMat gs = zipWith (zipWith (||)) mat $ transpose mat
      where mat = [replicate pad False ++ [acommQ g $ head gs' | g <- tail gs'] | (pad,gs') <- zip [1..] $ init $ tails gs]
    -- local stabilizers cut by [x..x+lx/2-1] where 0 <= i < lx/2 due to symmetry
    -- TODO: I think this works even if lx is odd, but I'm not certain
    cutStabs :: IntMap [Sigma]
    cutStabs = IntMap.unionsWith (flip (++))
             $ map (\g -> IntMap.fromListWith (error "cutStabs'")
                        $ map (,[g]) $ cutRegions $ intUnion $ map (flip div lY) $ IntMap.keys $ ik'G g) localStabs
      where cutRegions :: [Int] -> [Int]
            cutRegions xs = concatMap region0 $ zip xs (tail xs ++ [head xs])
            region0 (x,x') | modx (x'-x) <= lx_2 = (region `on` modx) (x      +1) (x'     +1)
                           | otherwise           = (region `on` modx) (x'-lx_2+1) (x +lx_2+1)
            region :: Int -> Int -> [Int]
            region x x' = x<=x' ? [x..x'-1] $ [0..x'-1] ++ [x..lx-1]
            lx_2 = div lx 2
    -- local and nonlocal stabilizers
    localStabs, nonlocalStabs :: [Sigma]
    [localStabs, nonlocalStabs] = [toList $ NestedFold $ cgs stab | cgs <- [lcgs'Ham, nlcgs'Ham]]

randoms :: Int -> [F]
randoms seed = map toF $ Random.randoms $ Random.mkStdGen seed

i_xs :: [Int] -> [Int] -> Int
i_xs ls xs = foldl' (\i (l,x) -> i*l + mod x l) 0 $ zip ls xs

xs_i :: [Int] -> Int -> [Int]
xs_i ls i0 = map snd $ init $ scanr (\l (i,_) -> divMod i l) (i0,0) ls

-- site -> randoms -> (SigmaTerms, unused_randoms)
type ModelGen = [Int] -> [F] -> ([([([Int],Int)],F)], [F])

init_generic'Ham :: Int -> ([Int],ModelGen) -> Ham
init_generic'Ham seed (ls,model) = fromLists'Ham ls 0 $ mapMaybe f $ concat
                                $ flip State.evalState (randoms seed) $
                                  mapM (State.state . model) $ mapM (\l -> [0..l-1]) ls
  where f :: ([([Int],Int)],F) -> Maybe ([(Int,Int)],F)
        f (_,0) = Nothing
        f (g,c) = Just (flip map g $ mapFst $ i_xs ls, c)

data Model = RandFerm | Ising | XYZ | XYZ2 | MajChain | ToricCode | Z2Gauge
  deriving (Eq, Show, Read, Enum)

model_gen :: Model -> [Int] -> [F] -> ([Int],ModelGen)
model_gen RandFerm ls [p] = (ls, gen)
  where n          = product ls
        gen [0] rs = foldl' gen' ([],rs) $
                       [ [([x],3)] | x <- [0..n-1] ] ++
                       [ [([x1],k1),([x2],k2)] ++ [([x],3) | x <- [x1+1..x2-1]] |
                         x1 <- [0..n-1], x2 <- [x1+1..n-1], k1 <- [1,2], k2 <- [1,2]]
        gen [_] rs = ([], rs)
        gen  _  _  = error "model_gen RandFerm"
        gen' (terms,rp:rh:rs') g = (if' (rp<p) [(g,rh)] [] ++ terms, rs')
        gen' _                 _ = error "model_gen RandFerm gen'"
model_gen Ising ls [j,k,h] = (ls, gen)
  where (kj,kh) = (3,1)
        gen [x] (rj:rk:rh:rs) =
          ([ ([([x],kj),([x+1],kj)],j*rj), ([([x],kh),([x+1],kh)],k*rk), ([([x],kh)],h*rh) ], rs)
        gen [x,y] (rjx:rjy:rkx:rky:rh:rs) =
          ([ ([([x,y],kj),([x+1,y  ],kj)],j*rjx), ([([x,y],kh),([x+1,y  ],kh)],k*rkx), ([([x,y],kh)],h*rh),
             ([([x,y],kj),([x  ,y+1],kj)],j*rjy), ([([x,y],kh),([x  ,y+1],kh)],k*rky) ], rs)
        gen _ _ = error "model_gen Ising"
model_gen XYZ ls j = (ls, gen)
  where gen [x] rs_ = let (r,rs) = splitAt 3 rs_ in
          ([ ([([x],1+k),([x+1],k+1)], j!!k * r!!k) | k<-[0..2]], rs)
        gen _ _ = error "model_gen XYZ"
model_gen XYZ2 ls js | length js == 3*3 = (ls, gen) -- [jx,jy,jz,jx2,jy2,jz2,jx',jy',jz']
  where gen [x] rs_ = let [j,j2,j'] = partitions 3 js
                          ([r,r2,r'],rs) = mapFst (partitions 3) $ splitAt (3*3) rs_ in
          (concat [concat [
            [([([x  ],k+1),([x+1],k+1)], j !!k * r !!k),
             ([([x-1],k+1),([x+1],k+1)], j2!!k * r2!!k)],
            [([([x+i],mod (k+p*i) 3 +1) | i<-[-1..1]], j'!!k * r'!!k) | p<-[-1,1]] ]| k<-[0..2]], rs)
        gen _ _ = error "model_gen XYZ"
model_gen XYZ2 ls [jx,jy,jz] = model_gen XYZ2 ls [jx,jy,jz,0.1,0,0,0,0,0]
-- model_gen XYZ2 ls j@[_,_,_] = model_gen XYZ2 ls $ concat [map (*f) j | f <- [1,0.5,0]]
-- model_gen MajChain [lx] couplings = model_gen MajChain [lx,1] couplings
-- model_gen MajChain ls [t,t',h] = (ls++[3], gen)
--   where (k1,k3, kh) = (1,3, 3)
--         gen [i,a,0] (rt1:rt2:rt'1:rt'2:rh1:rh2:rs) =
--             ([ (             ik1,t *rt1 ), (             ik2, t *rt2 ),   ([([i,a,1],kh)],-h*rh1),
--                (([i,a,1],kh):ik1,t'*rt'1), (([i,a,2],kh):ik2,-t'*rt'2),   ([([i,a,2],kh)],-h*rh2) ], rs)
--           where ik1 = [([i,a,0],k3)]
--                 ik2 = [([i,a,0],k1),([i+1,a,0],k1)]
--         gen [_,7,_] _  = error "model_gen MajChain 7"
--         gen [_,_,_] rs = ([], rs)
--         gen _ _ = error "model_gen MajChain"
model_gen MajChain [lx] [t,   g   ] = model_gen MajChain [lx] [t,t,g,g]
model_gen MajChain [lx] [t,t',g,g'] = ([lx], gen)
  where (kt,kt') = (3,1) -- corr_z_xs_ggs depends on this
        gen [x] (rt:rt':rg:rg':rs) =
          ([  ([([x],kt )            ],  t *rt ),
              ([([x],kt'),([x+1],kt')],  t'*rt'),
              ([([x],kt ),([x+1],kt )],  g *rg ),
              ([([x],kt'),([x+2],kt')],  g'*rg')
            ], rs)
        gen _ _ = error "model_gen MajChain"
model_gen ToricCode ls [a,b',b,a'] = (ls++[2], gen)
  where (ka,kb) = (3,1)
        gen [x,y,0] (ra:rb:ra'1:ra'2:rb'1:rb'2:rs) =
          ([ ([([x,y,1],ka),([x+1,y,2],ka),([x,y+1,1],ka),([x,y,2],ka)],a*ra), ([([x,y,1],kb)],b'*rb'1), ([([x,y,2],kb)],b'*rb'2),
             ([([x,y,1],kb),([x-1,y,1],kb),([x,y-1,2],kb),([x,y,2],kb)],b*rb), ([([x,y,1],ka)],a'*ra'1), ([([x,y,2],ka)],a'*ra'2)], rs)
        gen [_,_,1] rs = ([],rs)
        gen _       _  = error "model_gen ToricCode"
model_gen Z2Gauge ls [a,b',b,a'] = (ls++[3], gen)
  where (ka,kb,kA,kB) = (3,1,3,1)
        c = sum $ map abs $ [a,b',b,a']
        gen [x,y,0] (ra:rb:ra'1:ra'2:rb'1:rb'2:rs) =
          ([ ([([x,y,1],ka),([x+1,y,2],ka),([x,y+1,1],ka),([x,y,2],ka)],a*ra), ([([x,y,1],kb)],b'*rb'1), ([([x,y,2],kb)],b'*rb'2),
             ([([x,y,0],kB)],b*rb), ([([x,y,0],kA),([x,y,1],ka),([x+1,y  ,0],kA)],a'*ra'1),
                                    ([([x,y,0],kA),([x,y,2],ka),([x  ,y+1,0],kA)],a'*ra'2),
             ([([x,y,0],kB),([x,y,1],kb),([x-1,y,1],kb),([x,y-1,2],kb),([x,y,2],kb)],c)], rs)
        gen [_,_,_] rs = ([],rs)
        gen _       _  = error "model_gen Z2Gauge"
model_gen _ _ _ = error "model_gen"

model_gen_apply_gamma :: Double -> ([Int],ModelGen) -> ([Int],ModelGen)
model_gen_apply_gamma gamma (ls,gen) = (ls, mapFst (map $ mapSnd $ gamma==0 ? (boole . (/=0)) $ (`powF` gamma)) .* gen)

generic_histo_ :: [Int] -> [(Int,a)] -> [(Int,[a])]
generic_histo_ xs = IntMap.toList . foldl'
                      (\hist (x,y) -> (flip $ IntMap.alter $ Just . (y:) . fromJust) hist
                                      $ fst $ fromJust $ IntMap.lookupGE x hist)
                      (IntMap.fromListWith undefined $ map (,[]) xs)

toF    :: Double -> F
type_F :: String
powF   :: F -> Double -> F -- preserves sign of base
logF   :: F -> Double

-- type F = Double
-- type_F = "Double"
-- toF    = id
-- powF   = \x y -> signum x * (abs x ** y)
-- logF   = log

type F = LogFloat
type_F = "LogFloat"
toF    = fromDouble'LF
powF   = pow'LF
logF   = log'LF

prettyPrint :: Bool
prettyPrint = False

-- test_ham :: Ham
-- test_ham = fromLists'Ham [2] 0 [([(0,3)],4), ([(0,1)],3), ([(0,1),(1,3)],2), ([(0,3),(1,3)],1)] 

main :: IO ()
main = do
  let ternaryQ       = False
      small_lsQ      = False
      calc_EEQ       = True
      calc_aCorrQ    = True
      calc_aCorr'Q   = True
      detailedQ      = False
      cut_powQ       = True
      
      max_rg_terms_default    = calc_aCorr'Q ? "32" $ "16"
      max_wolff_terms_default = "4"
  
  args <- getArgs
  when (length args < 5) $ do
    putStrLn $ "usage: SBRG random-seed model [" ++ if' small_lsQ "" "ln2 " ++ "system lengths] [coupling constants] gamma"
            ++ " {max RG terms = " ++ max_rg_terms_default ++ "} {max wolff terms = " ++ max_wolff_terms_default ++ "}"
    putStr   $ "available models: "
    print    $ enumFrom $ (toEnum 0 :: Model)
    exitFailure
  
  (seed,model,ln2_ls,couplings,gamma,max_rg_terms_,max_wolff_terms_) <- getArgs7 [max_rg_terms_default, max_wolff_terms_default]
    :: IO (Int, Model, [Int], [F], Double, Int, Int)
  
  let max_rg_terms    = justIf (>=0) max_rg_terms_
      max_wolff_terms = justIf (>=0) max_wolff_terms_
      ls0      = small_lsQ ? ln2_ls $ map (2^) ln2_ls
      ls       = ls'RG rg0
      n        = n'RG  rg0
      z        = n // product ls0
      rg0      = init'RG $ init_generic'Ham seed $ model_gen_apply_gamma gamma $ model_gen model ls0 couplings
      rg       = runRG $ rg0 { diag'RG            = not ternaryQ ? diag'RG rg0 $ Nothing,
                               trash'RG           = Nothing,
                               max_rg_terms'RG    = max_rg_terms,
                               max_wolff_terms'RG = max_wolff_terms }
      xs       = ternaryQ  ? [head ls//2]
               $ (small_lsQ ? id $ filter isPow2) [1..head ls//2]
    --xs_small = [1..min (div (head ls0+1) 2) (2*head ln2_ls)]
  
  unless (0 < n) $ error "ln2_ls"
  
  putStr "version:            "; print (160324.0 :: Double) -- year month day . minor
  putStr "model:              "; print $ show model
  putStr "Ls:                 "; print ls0
  putStr "couplings:          "; print couplings
  putStr "Γ:                  "; print gamma
  putStr "seed:               "; print seed
  putStr "max RG terms:       "; print max_rg_terms
  putStr "max Anderson terms: "; print max_wolff_terms
  putStr "float type:         "; print type_F
  
  when detailedQ $ do
    putStr "Hamiltonian: "
    print $ ham0'RG rg0
  
  putStr "missing stabilizers: "
  print $ length $ takeWhile ((==0) . snd) $ stab0'RG rg
  
  let cut_pow2 = reverse . if' (cut_powQ && not small_lsQ) (filter $ isPow2 . fst) id . zip [1..]
  
  unless ternaryQ $ do
    putStr "Wolff errors: "
    print $ cut_pow2 $ wolff_errors'RG rg
    
  --putStr "Wolff2 errors: "
  --print $ cut_pow2 $ wolff2_errors'RG rg
    
    putStr "offdiag errors: "
    print $ cut_pow2 $ offdiag_errors'RG rg
    
    putStr "stabilizer energies: "
    print $ cut_pow2 $ map snd $ stab0'RG rg
    
    putStr "stabilizer sizes: "
    print $ cut_pow2 $ map (IntMap.size . ik'G . fst . c4s (-1) (g4s'RG rg)) $ stab0'RG rg
    
    putStr "stabilizer lengths: "
    print $ cut_pow2 $ map (length'Sigma (head ls) . fst . c4s (-1) (g4s'RG rg)) $ stab0'RG rg
  
  when detailedQ $ do
    putStr "stabilizers: "
    print $ stab'RG rg
    
    putStr "stabilizers0: "
    print $ stab0'RG rg
    
    putStr "stabilizers1: "
    print $ stab1'RG rg
    
    putStr "i3s: "
    print $ i3s'RG rg
    
    flip mapM_ (diag'RG rg) $ \diag -> do
      putStr "effective Hamiltonian: "
      print diag
    
    putStr "holographic Hamiltonian: "
    print $ c4_ham0'RG rg
  
  unless ternaryQ $ do
    let log_ns      = reverse $ takeWhile (/=0) $ iterate (flip div 2) n
        small_ns n0 = n<=n0 ? [1..n] $ [1..n0]++[n]
        
        -- [(bin upper bound, #, RSS of coefficients, sum of logs, max)]
        generic_histo :: [Int] -> (Sigma -> Int) -> [(Sigma,F)] -> [(Int,Int,F,Double,F)]
        generic_histo ns f = map (\(n0,cs) -> (n0,length cs,rss cs, sum $ map (logF . abs) $ filter (/=0) cs, maximum $ map abs $ 0:cs)) . generic_histo_ ns . map (mapFst f)
        
        length_histo :: [Int] -> [(Sigma,F)] -> [(Int,Int,F,Double,F)] -- TODO d>1
        length_histo ns = generic_histo ns $ length'Sigma l
          where l = head ls
        
        all_histos :: String -> ([Int],[Int]) -> [Int] -> [(Sigma,F)] -> IO ()
        all_histos name (nsS,nsS') nsL gcs = do
          putStr $ name ++ " size histo: "
          print $ generic_histo nsS (IntMap.size . ik'G) gcs
          
          when (length ls0 == 1) $ do
            putStr $ name ++ " length histo: "
            print $ length_histo nsL gcs
            
            putStr $ name ++ " size-length histo: "
            print $ map (mapSnd $ length_histo nsL) $ generic_histo_ nsS' $ map (\gT@(g,_) -> (IntMap.size $ ik'G g, gT)) gcs
    
    putStr "small stabilizers: " 
    print $ map (mapFst $ IntMap.size . ik'G)
          $ uncurry (++) $ mapSnd (take 20) $ span ((==0) . snd) $ sortWith (abs . snd) $ toList'Ham $ stab'RG rg
    
    all_histos "stabilizer"      (log_ns     , log_ns     ) log_ns         $ Map.toList  $  gc'Ham  $  stab'RG rg
    all_histos "diag"            (small_ns 32, small_ns 16) log_ns & mapM_ $ Map.toList <$> gc'Ham <$> diag'RG rg
--  all_histos "c4 ham0"         (log_ns     , log_ns     ) log_ns         $ Map.toList  $  gc'Ham  $   c4_ham0'RG rg 
--  all_histos "diag c4 ham0"    (log_ns     , log_ns     ) log_ns         $ filter (all (==3) . ik'G . fst) $ Map.toList  $ gc'Ham  $   c4_ham0'RG rg 
--  all_histos "offdiag c4 ham0" (log_ns     , log_ns     ) log_ns         $ filter (not . all (==3) . ik'G . fst) $ Map.toList  $ gc'Ham  $   c4_ham0'RG rg 
  
  when calc_EEQ $ do
    putStr "entanglement entropy: " -- [(region size, region separation, entanglement entropy, error)]
    print $ ee1d xs (small_lsQ || ternaryQ ? [0] $ 0:xs) $ stab'RG rg
  
--   putStr "entanglement entropies: " -- [(region size, region separation, [entanglement entropies])]
--   print $ ee1d_ [1..last xs] [0] $ stab'RG rg
  
  let corr_z_xs_ggs = mapThd3 (map $ mapBoth $ sigma 0 . IntMap.fromListWith undefined) $ case model of
        ToricCode -> wilson_z_xs_ggs
        Z2Gauge   -> wilson_z_xs_ggs
        MajChain  -> let (a,b,c,d) = ([(0,3)], [(0,1),(1,1)], [(0,3),(1,3)], [(0,1),(2,1)])
                     in  (1, xs, [(a,a), (b,b), (b,a), (c,c), (d,d), (d,c)])
        _         -> (z, small_lsQ || ternaryQ ? xs $ sort $ union xs $ map (+(-1)) $ tail xs, xyz_gs)
      xyz_gs = copy [[(0,k)] | k <- [1..3]]
      wilson_z_xs_ggs = (last ls0*z, xs,
                         copy [[(i_xs ls [0,y,mu],k) | y <- [0..last ls0-1]] | (mu,k) <- [(1,1),(2,3)]])
      copy   = map $ \x -> (x,x)
      aCorr  = uncurry3 anderson_corr  corr_z_xs_ggs $ stab'RG rg
      aCorr' = uncurry3 anderson_corr' corr_z_xs_ggs           rg
  
  when calc_aCorrQ $ do
    putStr "anderson correlator:  " -- [(distance, [xyz -> (correlator,error)])]
    print $ aCorr
  
  when calc_aCorr'Q $ do
    putStr "anderson correlator': " -- [(distance, [xyz -> (correlator,error)])]
    max_wolff_terms_==0 ? print aCorr $ print aCorr'
  
  putStr "CPU time: "
  cpu_time <- getCPUTime
  print $ (1e-12::Double) * fromIntegral cpu_time

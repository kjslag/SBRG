-- author: Kevin Slagle
-- github.com/kjslag/SBRG

-- SBRG2.hs uses Majorana fermions while SBRG.hs uses spins

-- stack install hashable NumInstances ieee754 safe clock parallel random strict

{-# LANGUAGE CPP, TupleSections, BangPatterns, MagicHash, MultiParamTypeClasses, FlexibleInstances, GeneralizedNewtypeDeriving, DeriveFunctor, DeriveFoldable #-} -- OverloadedLists
-- :set -XTupleSections

{-# OPTIONS_GHC -Wall -Wno-unused-top-binds -Wno-unused-imports -Wno-orphans -O #-} -- -fllvm -fexcess-precision -optc-ffast-math -optc-O3
-- -rtsopts -prof -fprof-auto        -ddump-simpl -threaded
-- +RTS -xc -s -p                    -N4
-- +RTS -hy && hp2ps -c SBRG.hp && okular SBRG.ps

-- ghci -fbreak-on-error
-- :set -fbreak-on-error
-- :set args 1 XYZ [4] [1,1,1] 1
-- :trace main

-- default:
#ifndef FERMIONIC
#ifndef BOSONIC
#define BOSONIC
#endif
#endif

import GHC.Exts (groupWith, sortWith, the)

import Control.Applicative
-- import Control.Exception.Base (assert)
import Control.Monad
import Data.Bifunctor
import Data.Bits
import Data.Coerce
import Data.Either
import Data.Function
import Data.Foldable
import Data.Hashable
import Data.List
import Data.Maybe
import Data.Monoid
import Data.NumInstances.Tuple
import Data.Ord
import Data.Tuple
import Debug.Trace
import Numeric.IEEE (nan, infinity, succIEEE, predIEEE)
import Safe
import Safe.Exact
import System.Environment
import System.Exit (exitSuccess, exitFailure)

import qualified System.CPUTime as CPUTime
import qualified System.Clock as Clock
import qualified GHC.Stats as GHC

import qualified Control.Parallel as Parallel
import qualified Control.Parallel.Strategies as Parallel

import qualified System.Random as Random

import Data.Strict (Pair(..))
import qualified Data.Strict as S

--import Control.Monad.Trans.State.Strict (State)
import qualified Control.Monad.Trans.State.Strict as State

import Data.IntSet (IntSet)
import qualified Data.IntSet as IntSet

import Data.Set (Set)
import qualified Data.Set as Set

import Data.IntMap.Strict (IntMap)
import qualified Data.IntMap.Strict as IntMap hiding (fromList, insert, delete, adjust, adjustWithKey, update, updateWithKey) -- use alter instead

import Data.Map.Strict (Map)
import qualified Data.Map.Strict as  Map hiding (fromList, insert, delete, adjust, adjustWithKey, update, updateWithKey) -- use alter instead
import qualified Data.Map.Lazy   as LMap hiding (fromList, insert, delete, adjust, adjustWithKey, update, updateWithKey)

-- import Data.Vector.Unboxed (Vector)
-- import qualified Data.Vector.Unboxed as Vector

-- debug

-- error_ :: a
-- error_ = error "compile with '-rtsopts -prof -fprof-auto' and run with '+RTS -xc' for a stack trace"

todo :: a
todo = undefined

assert :: Bool -> a -> a
assert True  = id
assert False = error "assert"

asserting :: (a -> Bool) -> a -> a
asserting q x = assert (q x) x

-- generic

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
-- fromLeft _        = undefined

-- fromRight :: Either a b -> b
-- fromRight (Right y) = y
-- fromRight _         = undefined

infixl 7 //
(//) :: Integral a => a -> a -> a
x // y | (q,0) <- divMod x y = q
       | otherwise           = undefined

fst3 :: (a,b,c) -> a
fst3 (x,_,_) = x

snd3 :: (a,b,c) -> b
snd3 (_,y,_) = y

thd3 :: (a,b,c) -> c
thd3 (_,_,z) = z

both :: (a -> b) -> (a,a) -> (b,b)
both f = bimap f f

first3 :: (a -> a') -> (a,b,c) -> (a',b,c)
first3 f (x,y,z) = (f x,y,z)

second3 :: (b -> b') -> (a,b,c) -> (a,b',c)
second3 f (x,y,z) = (x,f y,z)

third3 :: (c -> c') -> (a,b,c) -> (a,b,c')
third3 f (x,y,z) = (x,y,f z)

uncurry3 :: (a -> b -> c -> d) -> (a, b, c) -> d
uncurry3 f (x,y,z) = f x y z

mapHead :: (a -> a) -> [a] -> [a]
mapHead f (x:xs) = f x : xs
mapHead _ _      = undefined

foldl'_ :: Foldable t => (a -> b -> b) -> t a -> b -> b
foldl'_ = flip . foldl' . flip

partitions :: Int -> [a] -> [[a]]
partitions _ [] = []
partitions n xs = uncurry (:) $ second (partitions n) $ splitAtExact n xs

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

getArgs6 :: (Read t1, Read t2, Read t3, Read t4, Read t5, Read t6) => [String] -> IO (t1,t2,t3,t4,t5,t6)
getArgs6 defaults = do
  args <- getArgs
  let n = 6
      [x1,x2,x3,x4,x5,x6] = args ++ drop (length defaults + length args - n) defaults
  return (readNote "1" x1, readNote "2" x2, readNote "3" x3, readNote "4" x4, readNote "5" x5, readNote "6" x6)

getArgs7 :: (Read t1, Read t2, Read t3, Read t4, Read t5, Read t6, Read t7) => [String] -> IO (t1,t2,t3,t4,t5,t6,t7)
getArgs7 defaults = do
  args <- getArgs
  let n = 7
      [x1,x2,x3,x4,x5,x6,x7] = args ++ drop (length defaults + length args - n) defaults
  return (readNote "1" x1, readNote "2" x2, readNote "3" x3, readNote "4" x4, readNote "5" x5, readNote "6" x6, readNote "7" x7)

infixl 1 `applyIf`
applyIf :: (a -> a) -> (a -> Bool) -> a -> a
applyIf f q x = if' (q x) f id $ x

nest :: Int -> (a -> a) -> a -> a
nest 0 _ = id
nest n f = nest (n-1) f . f

justIf :: Bool -> a -> Maybe a
justIf True  = Just
justIf False = const Nothing

justIf' :: (a -> Bool) -> a -> Maybe a
justIf' f x = justIf (f x) x

minimumOn :: Ord b => (a -> b) -> [a] -> a
minimumOn f xs = snd $ minimumBy (comparing fst) $ map (\x -> (f x, x)) xs

maximumOn :: Ord b => (a -> b) -> [a] -> a
maximumOn f xs = snd $ maximumBy (comparing fst) $ map (\x -> (f x, x)) xs

intUnion :: [Int] -> [Int]
intUnion = IntSet.toList . IntSet.fromList

unions :: Eq a => [[a]] -> [a]
unions = foldl' union []

xor'IntSet :: IntSet -> IntSet -> IntSet
xor'IntSet x y = IntSet.union x y `IntSet.difference` IntSet.intersection x y

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

mergeSortedWith :: Ord b => (a -> b) -> [[a]] -> [a]
mergeSortedWith f = mergeSortedBy $ comparing f

-- same as mergeSortedBy except uses f to combine equal elements
mergeUnionsBy :: (a -> a -> Ordering) -> (a -> a -> a) -> [[a]] -> [a]
mergeUnionsBy cmp f = mergeUsing mergePair []
  where mergePair xs@(x:xs') ys@(y:ys') = case x `cmp` y of
                                               LT ->   x   : mergePair xs' ys
                                               GT ->     y : mergePair xs  ys'
                                               EQ -> f x y : mergePair xs' ys'
        mergePair [] ys = ys
        mergePair xs [] = xs

-- mergeUnionsWith :: Ord b => (a -> b) -> (a -> a -> a) -> [[a]] -> [a]
-- mergeUnionsWith f = mergeUnionsBy $ comparing f

sq :: Num a => a -> a
sq x = x*x

isPow2 :: Int -> Bool
isPow2 x = popCount x == 1

mean :: Floating f => [f] -> f
mean = (\(n,x) -> x/fromIntegral n) . sum . map (1::Int,)

rms :: Floating f => [f] -> f
rms = sqrt . mean . map sq

rss :: Floating f => [f] -> f
rss = sqrt . sum . map sq

weightedMeanError :: (Floating f, MySeq f) => [(f, f)] -> (f, f)
weightedMeanError = weightedMeanError' (*)

weightedMeanError' :: (Fractional f, MySeq f, Floating v, MySeq v) => (f -> v -> v)  -> [(f, v)] -> (v, v)
weightedMeanError' (*&) = (\(rw,wx,wxx) -> (rw*&wx, rw *& sqrt (wxx - rw*&(wx*wx))))
                     . first3 (recip) . foldl' (myForce .* (+)) 0 . map (\(w,x) -> (w,w*&x,w*&(x*x)))

meanError :: Floating f => [f] -> (f, f)
meanError = (\(n,x,xx) -> (x/n, sqrt ((n*xx - x*x)/(n-1)) / n))
          . first3 fromIntegral . sum . map (\x -> (1::Int,x,x*x))

epsilon, epsilon_2 :: Fractional f => f
epsilon   = 2 ^^ (-52::Int)
epsilon_2 = 2 ^^ (-26::Int)

-- Nothing if x+y is tiny compared to x and y
infixl 6 +?
(+?) :: F -> F -> Maybe F
x +? y = x_y*x_y < epsilon*(x*x + y*y) ? Nothing $ Just x_y
  where x_y = x + y

rotateLeft :: Int -> [a] -> [a]
rotateLeft n xs = bs ++ as
  where (as,bs) = splitAtExact n xs

-- input must be sorted
modDifferences :: Int -> [Int] -> [Int]
modDifferences l is = (head is + l - last is) : zipWith (-) (tail is) is

modDist :: Int -> Int -> Int
modDist l x = abs $ mod (x+l//2) l - l//2

myParListChunk :: MySeq a => Int -> [a] -> [a]
myParListChunk n | parallelQ = Parallel.withStrategy $ Parallel.parListChunk n $ Parallel.rseq . myForce
                 | otherwise = id

par, pseq :: a -> b -> b
par  = parallelQ ? Parallel.par  $ flip const
pseq = parallelQ ? Parallel.pseq $ flip const

-- Force

class MySeq a where
  mySeq :: a -> b -> b
  mySeq = seq
  
  myForce :: a -> a
  myForce x = x `mySeq` x

mySeq'Foldable :: (Foldable t, MySeq a) => t a -> b -> b
mySeq'Foldable = foldl'_ mySeq

instance MySeq Int
instance MySeq Double

instance MySeq a => MySeq (Maybe a) where
  mySeq = mySeq'Foldable

instance MySeq a => MySeq [a] where
  mySeq = mySeq'Foldable

instance (MySeq a, MySeq b) => MySeq (a,b) where
  mySeq (a,b) = mySeq a . mySeq b

instance (MySeq a, MySeq b, MySeq c) => MySeq (a,b,c) where
  mySeq (a,b,c) = mySeq a . mySeq b . mySeq c

instance (MySeq a, MySeq b, MySeq c, MySeq d) => MySeq (a,b,c,d) where
  mySeq (a,b,c,d) = mySeq a . mySeq b . mySeq c . mySeq d

instance (MySeq a, MySeq b) => MySeq (Either a b) where
  mySeq = either mySeq mySeq

-- LogFloat
-- https://hackage.haskell.org/package/logfloat-0.13.3.1/docs/Data-Number-LogFloat.html#v:product

data LogFloat = LogFloat !Bool !Double

toDouble'LF :: LogFloat -> Double
toDouble'LF (LogFloat s y) = s ? exp y $ -exp y

fromDouble'LF :: Double -> LogFloat
fromDouble'LF x = LogFloat (not $ x<0 || isNegativeZero x) (log $ abs x)

-- preserves sign of base
pow'LF :: LogFloat -> Double -> LogFloat
pow'LF (LogFloat s y) x = LogFloat s $ y*x
-- pow'LF _ _ = undefined

exp'LF :: Double -> LogFloat
exp'LF x = LogFloat True x

log'LF :: LogFloat -> Double
log'LF (LogFloat True y) = y
log'LF _                 = undefined

isNaN'LF :: LogFloat -> Bool
isNaN'LF (LogFloat _ y) = isNaN y

succIEEE'LF :: LogFloat -> LogFloat
succIEEE'LF (LogFloat False y) = LogFloat False $ predIEEE y
succIEEE'LF (LogFloat True  y) = LogFloat True  $ succIEEE y

instance MySeq LogFloat

instance Show LogFloat where
  showsPrec n z@(LogFloat s y) str
    | abs y > 600 && not (isInfinite y || isNaN y)
      = if' s "" "-" ++ (abs y > recip epsilon
                        ? "10^" ++ shows (y/ln10) str
                        $ shows (exp $ ln10*y') ('e' : shows (e::Integer) str) )
    | otherwise = showsPrec' n (toDouble'LF z) str
    where (e,y') = properFraction $ y / ln10
          ln10   = log 10
          showsPrec' a b = showsPrec a b

instance Read LogFloat where
  readsPrec = map (first fromDouble'LF) .* readsPrec

instance Eq LogFloat where
  (==) = (==EQ) .* compare

instance Ord LogFloat where
  compare (LogFloat s1 y1) (LogFloat s2 y2)
    |    y1 == -infinity
      && y2 == -infinity   = EQ
    | isNaN y1 || isNaN y2 = undefined -- GT -- http://stackoverflow.com/a/6399798
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
    | otherwise           = error (show ("nan",s1,y1,s2,y2)) $ LogFloat True nan
      where dy = y1 - y2
  (LogFloat s1 y1) * (LogFloat s2 y2) = LogFloat (not $ xor s1 s2) (y1 + y2)
  abs (LogFloat _ y) = LogFloat True y
  signum z@(LogFloat s y)    = (isInfinite y && y < 0) || isNaN y ? z $ LogFloat s 0
  fromInteger = fromDouble'LF . fromInteger
  negate (LogFloat s y) = LogFloat (not s) y

-- check'LF :: Bool
-- check'LF = and [isNaN z_ == isNaN'LF z' && (isNaN z_ || (z-eps <= z' && z' <= z+eps && (1/z_>0) == (1/z'>0) && compare x_ y_ == compare x y))
--            | x_ <- xs,
--              y_ <- xs,
--              let [x,y,z] = map fromDouble'LF [x_,y_,z_]
--                  z_      = x_ + y_
--                  z'      = x  + y
--                  eps = 1e-10]
--   where xs = [-infinity,-2,-1,-0,0,1,2,infinity,nan]

instance Fractional LogFloat where
  fromRational = fromDouble'LF . fromRational
  recip (LogFloat s y) = LogFloat s (-y)

instance Floating LogFloat where
  sqrt (LogFloat True y) = LogFloat True (y/2)
  sqrt _                 = undefined
  pi    = fromDouble'LF pi
  exp   = undefined
  log   = undefined
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

instance Hashable LogFloat where
  hashWithSalt n (LogFloat s y) = hashWithSalt n (s,y)

-- Z2Matrix

data Z2Mat = Z2Mat (IntMap IntSet) (IntMap IntSet)

fromSymmetric'Z2Mat :: IntMap IntSet -> Z2Mat
fromSymmetric'Z2Mat mat = Z2Mat mat mat

popRow'Z2Mat :: Z2Mat -> Maybe (IntSet, Z2Mat)
popRow'Z2Mat (Z2Mat rows cols) = case IntMap.minViewWithKey rows of
    Nothing               -> Nothing
    Just ((i,row), rows') -> let cols' = IntSet.foldl' (flip $ IntMap.alter $ justIf' (not . IntSet.null) . IntSet.delete i . fromJust) cols row
                             in Just (row, Z2Mat rows' cols')

xorRow'Z2Mat :: IntSet -> Int -> Z2Mat -> Z2Mat
xorRow'Z2Mat row i (Z2Mat rows cols) = Z2Mat rows' cols'
  where rows'     = IntMap.alter (xorMay row . fromJust) i rows
        cols'     = IntSet.foldl' (flip $ IntMap.alter $ maybe (Just i0) $ xorMay i0) cols row
        i0        = IntSet.singleton i
        xorMay    = justIf' (not . IntSet.null) .* xor'IntSet

-- rankZ2

rankZ2 :: IntMap IntSet -> Int
rankZ2 = go 0 . fromSymmetric'Z2Mat
  where
    go :: Int -> Z2Mat -> Int
    go !n mat_ = case popRow'Z2Mat mat_ of
        Nothing             -> n
        Just (row, mat@(Z2Mat _ cols)) -> go (n+1) $
            let j  = IntSet.findMin row
            in case IntMap.lookup j cols of
                    Nothing -> mat
                    Just is -> IntSet.foldl' (flip $ xorRow'Z2Mat row) mat is

rankZ2' :: [[Bool]] -> Int
rankZ2' = rankZ2 . IntMap.fromListWith undefined . zip [0..] . map (IntSet.fromList . map fst . filter snd . zip [0..])

boolsToInteger :: [Bool] -> Integer
boolsToInteger = foldl' (\x b -> 2*x + boole b) 0

symmeterize_rankZ2_old :: [[Bool]] -> Int
symmeterize_rankZ2_old mat = rankZ2_old' $ (zipWithExact (+) `on` map boolsToInteger) mat (transpose mat)

rankZ2_old :: [[Bool]] -> Int
rankZ2_old = rankZ2_old' . map boolsToInteger

rankZ2_old' :: [Integer] -> Int
rankZ2_old' = go 0
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

instance MySeq AbsF

instance Ord AbsF where
  AbsF x `compare` AbsF y = case compare (abs y) (abs x) of
                                 EQ -> compare y x
                                 o  -> o

-- Sigma

type Index = Int
type Pos   = Int
type SigmaHash = Int

-- if bosonic, 00~1, 01~x, 10~y, 11~z
-- if fermionic, is'G should always have even size, and if size is 2 mod 4, then an implicit factor of i is included
data Sigma = Sigma {
  is'G   :: !IntSet, -- Index set
  hash'G ::  SigmaHash }

sigma :: IntSet -> Sigma
sigma ik = g
  where g = Sigma ik $ calc_hash'G g

check'G :: Sigma -> Bool
check'G g = bosonicQ || even (IntSet.size $ is'G g)

calc_hash'G :: Sigma -> SigmaHash
calc_hash'G = hash . toList'G

pos_i'G :: Index -> Pos
pos_i'G | fermionicQ = id
        | otherwise  = (`div` 2)

xs_i'G :: Ls -> Index -> Xs
xs_i'G ls = xs_pos ls . pos_i'G

positions'G :: Sigma -> IntSet
positions'G | fermionicQ = is'G
            | otherwise  = IntSet.map pos_i'G . is'G

-- http://hackage.haskell.org/package/hashable-1.2.4.0/docs/src/Data.Hashable.Class.html#line-452
instance Hashable Sigma where
  hashWithSalt salt = hashWithSalt salt . hash'G
  hash = hash'G

instance MySeq Sigma

instance Eq Sigma where
  Sigma g1 hash1 == Sigma g2 hash2 = hash1==hash2 && g1==g2

instance Ord Sigma where
  Sigma g1 hash1 `compare` Sigma g2 hash2 = compare hash1 hash2 <> compare g1 g2

instance Show Sigma where
  showsPrec n g = showsPrec n $ toList'G g

type SigmaTerm = (Sigma, F)

toList'G :: Sigma -> [Index]
toList'G = IntSet.toList . is'G

toList'GT :: SigmaTerm -> ([Index],F)
toList'GT = first toList'G

fromList'G :: [Index] -> Sigma
fromList'G = asserting check'G . sigma . IntSet.fromList

fromListB'G :: [(Pos,Int)] -> Sigma
fromListB'G = assert bosonicQ fromList'G . concat . map f
  where f (i,k) = case k of 0 -> []
                            1 -> [2*i]
                            2 -> [2*i+1]
                            3 -> [2*i, 2*i+1]
                            _ -> error "fromListB'G"

fromList'GT :: ([Pos],F) -> SigmaTerm
fromList'GT = first fromList'G

fromListB'GT :: ([(Int,Int)],F) -> SigmaTerm
fromListB'GT = first fromListB'G

show'GTs :: [SigmaTerm] -> String
show'GTs = if' prettyPrint (concat . intersperse " + " . map (\(g,c) -> show c ++ " σ" ++ show g)) show

sort'GT :: [(a,F)] -> [(a,F)]
sort'GT = sortWith (AbsF . snd)

scale'GT :: F -> SigmaTerm -> SigmaTerm
scale'GT x (g,c) = (g,x*c)

length'G :: Int -> Sigma -> Int
length'G l = (l-) . maximum . modDifferences l . IntSet.toList . is'G

size'G :: Sigma -> Int
size'G = IntSet.size . is'G

acommQ :: Sigma -> Sigma -> Bool
acommQ g1 g2 = odd $ IntSet.size $ is'G g1 `IntSet.intersection` f (is'G g2)
  where f | fermionicQ = id
          | otherwise  = IntSet.map $ \i -> even i ? i+1 $ i-1
-- TODO implement fast version

multSigma :: Sigma -> Sigma -> (Int,Sigma)
multSigma (Sigma g1_ _) (Sigma g2_ _) | bosonicQ  = (sB, sigma $ xor'IntSet g1_ g2_)
                                      | otherwise = (sF - sf gF, sigma gF) -- TODO second part is just an xor
  where sB = sum $ map (\i -> even i ? 1 $ -1) $ IntSet.toList $ IntSet.intersection g1_ $ IntSet.map (\i -> even i ? i+1 $ i-1) g2_
        -- TODO speed up
        
        (sF,gF) = second IntSet.fromAscList $ f (IntSet.size g1_) (IntSet.toAscList g1_) (IntSet.toAscList g2_) (sf g1_ + sf g2_, [])
        sf g = case IntSet.size g `mod` 4 of
                    0 -> 0
                    2 -> 1
                    _ -> error "multSigma"
        f :: Int -> [Int] -> [Int] -> (Int,[Int]) -> (Int,[Int])
        f n1 (i1:g1) (i2:g2) (s,g') | i1 < i2   = f (n1-1)     g1  (i2:g2) (s  ,i1:g')
                                    | i1 > i2   = f  n1    (i1:g1)     g2  (s' ,i2:g')
                                    | otherwise = f (n1-1)     g1      g2  (s'',   g')
          where s'  = s + 2 * boole (odd   n1)
                s'' = s + 2 * boole (odd $ n1 - 1)
        f _ g1 g2 (s,g') = (s, g1++g2++g')

-- i [a,b]/2
icomm :: Sigma -> Sigma -> Maybe SigmaTerm
icomm g1 g2 = case mod s 4 of
                   0 -> Nothing
                   2 -> Nothing
                   1 -> Just (g,-1)
                   3 -> Just (g, 1)
                   _ -> undefined
  where (s,g) = multSigma g1 g2

-- {a,b}/2
acomm :: Sigma -> Sigma -> Maybe SigmaTerm
acomm g1 g2 = case mod s 4 of
                   0 -> Just (g, 1)
                   2 -> Just (g,-1)
                   1 -> Nothing
                   3 -> Nothing
                   _ -> undefined
  where (s,g) = multSigma g1 g2

icomm'GT :: SigmaTerm -> SigmaTerm -> Maybe SigmaTerm
icomm'GT (g1,c1) (g2,c2) = scale'GT (c1*c2) <$> icomm g1 g2

acomm'GT :: SigmaTerm -> SigmaTerm -> Maybe SigmaTerm
acomm'GT (g1,c1) (g2,c2) = scale'GT (c1*c2) <$> acomm g1 g2

-- c4 g4 g = R^dg g R where R = (1 + i g4)/sqrt(2)
c4 :: Sigma -> Sigma -> Maybe SigmaTerm
c4 g4 g = icomm g g4

c4s :: F -> [Sigma] -> SigmaTerm -> SigmaTerm
c4s dir g4s gT = foldl' (\gT'@(!g',!c') g4 -> maybe gT' (scale'GT $ dir*c') $ c4 g4 g') gT g4s

localQ'Sigma :: Int -> Sigma -> Bool
localQ'Sigma n = \g -> size'G g < round max_n
  where max_n = sqrt $ fromIntegral n :: Double
--where max_n = log (fromIntegral n) / log 2 + 16 :: Double

-- MapGT

type MapGT = Map Sigma F

fromList'MapGT :: [SigmaTerm] -> MapGT
fromList'MapGT = fromListWith' (+)
  where fromListWith' x y = Map.fromListWith x y

instance MySeq MapGT

-- MapAbsSigmas

type MapAbsSigmas = Map AbsF (Set Sigma)

delete'MAS :: SigmaTerm -> MapAbsSigmas -> MapAbsSigmas
delete'MAS (g,c) = Map.alter (justIf' (not . Set.null) . Set.delete g . fromJust) (AbsF c)

toList'MAS :: MapAbsSigmas -> [SigmaTerm]
toList'MAS gs_ = [(g,c) | (AbsF c,gs) <- Map.toAscList gs_,
                          g <- Set.toList gs]

toSortedList'MASs :: [MapAbsSigmas] -> [SigmaTerm]
toSortedList'MASs gs_ = [ (g',c) | (AbsF c,gs) <- mergeUnionsBy (comparing fst) (\(c,s) (_,s') -> (c, union'Set s s')) $ map Map.toAscList gs_,
                             g' <- toList'Set gs ]
  where union'Set x y = Set.union x y
        toList'Set x = Set.toList x

-- Ham

-- MapAbsSigmas is used so that acommCandidates_sorted is ordered by coefficient so that max_wolff_terms cuts is more efficient
-- TODO get rid of that?
data Ham = Ham {
  gc'Ham    :: !MapGT,                 -- Hamiltonian:  sigma matrix  -> coefficient
  lcgs'Ham  :: !MapAbsSigmas,          -- |coefficient| ->     local sigma matrices
  nlcgs'Ham :: !MapAbsSigmas,          -- |coefficient| -> non-local sigma matrices
  icgs'Ham  :: !(IntMap MapAbsSigmas), -- local sigmas: site index (/2 if bosonicQ) -> |coefficient| -> local sigma matrices
  ls'Ham    :: !Ls }                   -- system lengths
  deriving (Show)

n'Ham :: Ham -> Int
n'Ham = product . ls'Ham

instance MySeq Ham

-- check'Ham :: Ham -> Bool
-- check'Ham ham = ham == (fromLists'Ham (ls'Ham ham) $ toLists'Ham ham) && all check'Sigma (Map.keys $ gc'Ham ham)

zero'Ham :: Ls -> Ham
zero'Ham ls = Ham Map.empty Map.empty Map.empty IntMap.empty ls

toList'Ham :: Ham -> [SigmaTerm]
toList'Ham = Map.toList . gc'Ham

toDescList'Ham :: Ham -> [SigmaTerm]
toDescList'Ham ham = toSortedList'MASs [lcgs'Ham ham, nlcgs'Ham ham]

fromList'Ham :: Ls -> [SigmaTerm] -> Ham
fromList'Ham ls = (zero'Ham ls +#)

fromLists'Ham :: Ls -> [([Index],F)] -> Ham
fromLists'Ham ls = fromList'Ham ls . map fromList'GT

null'Ham :: Ham -> Bool
null'Ham = Map.null . gc'Ham

insert'Ham :: SigmaTerm -> Ham -> Ham
insert'Ham gT@(!g,!c) ham@(Ham gc lcgs nlcgs icgs ls)
-- | c == 0       = ham -- this line would break stab'RG
  | isNothing c_ =
    Ham (Map.insertWith undefined g c gc)
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
                 in          IntMap.mergeWithKey (\_ gc0 _ -> Just $ gc0 +# gT) id id icgs $ IntMap.fromSet (const cgs_) $ positions'G g

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
        icgs'         = IntMap.differenceWith (\cgs _ -> justIf' (not . Map.null) $ delete'MAS gT cgs) icgs
                      $ IntMap.fromSet (const ()) $ positions'G g

deleteSigmas'Ham :: Foldable t => t Sigma -> Ham -> Ham
deleteSigmas'Ham = foldl'_ delete'Ham

acommSigmas :: Sigma -> Ham -> [SigmaTerm]
acommSigmas g = filter (acommQ g . fst) . acommCandidates g

acommSigmas_sorted :: Sigma -> Ham -> [SigmaTerm]
acommSigmas_sorted g = filter (acommQ g . fst) . acommCandidates_sorted g

acommCandidates :: Sigma -> Ham -> [SigmaTerm]
acommCandidates = (\(nlgs:lgs) -> toList'MAS nlgs ++ toSortedList'MASs lgs) .* acommCandidates_
  -- toSortedList'MASs is needs so that we take unions of g

acommCandidates_sorted :: Sigma -> Ham -> [SigmaTerm]
acommCandidates_sorted = toSortedList'MASs .* acommCandidates_

acommCandidates_ :: Sigma -> Ham -> [MapAbsSigmas]
acommCandidates_ g ham = nlcgs'Ham ham : IntMap.elems localSigmas
  where localSigmas = IntMap.restrictKeys (icgs'Ham ham) $ positions'G g

nearbySigmas :: Pos -> Ham -> MapAbsSigmas
nearbySigmas i ham = nearbySigmas' [i] ham

nearbySigmas' :: [Pos] -> Ham -> MapAbsSigmas
nearbySigmas' is ham = nlcgs'Ham ham +# [IntMap.lookup i $ icgs'Ham ham | i <- is]

-- the Maybe SigmaTerm has a +/- 1 coefficient
icomm_sorted'Ham :: SigmaTerm -> Ham -> [(Maybe SigmaTerm, F)]
icomm_sorted'Ham (g,c) ham = map (\(g',c') -> (icomm g g', c*c')) $ acommCandidates_sorted g ham

c4'Ham :: F -> Sigma -> Ham -> Ham
c4'Ham dir g4 ham = uncurry (+#) $ foldl' delSigma (ham,[]) $ map fst $ acommCandidates g4 ham
  where delSigma (!ham0,!gTs) g = case c4 g4 g of
                                       Nothing      -> (ham0,gTs)
                                       Just (g',c') -> second (\c -> (g',dir*c*c'):gTs) $ deleteLookup'Ham g ham0

c4s'Ham :: F -> [Sigma] -> Ham -> Ham
c4s'Ham dir = foldl'_ $ c4'Ham dir

-- c4_wolff'Ham :: Maybe Int -> F -> G4_H0G -> Ham -> Ham
-- c4_wolff'Ham = c4_wolff_'Ham False
-- 
-- -- Schrieffer-Wolff: H -> S^dg H S
-- --                      = H - (2h_3^2)^-1 [H, H_0 Sigma]
-- --                      = H + ( h_3^2)^-1 icomm H (icomm H_0 Sigma)
-- --                      = H +             icomm H _H0G
-- -- note: # of terms cut by max_wolff_terms is affected by iD'G
-- -- onlyDiag note: only local sigmas are guaranteed to be diagonal -- (maybe this isn't true anymore?)
-- c4_wolff_'Ham :: Bool -> Maybe Int -> F -> G4_H0G -> Ham -> Ham
-- c4_wolff_'Ham True     _               dir  _ _ | dir /= 1       = undefined
-- c4_wolff_'Ham _        _               dir (Left    g4     ) ham = c4'Ham dir g4 ham
-- c4_wolff_'Ham onlyDiag max_wolff_terms dir (Right (_H0G,i3)) ham = deleteSigmas'Ham dels ham
--                                                                  +# maybe id take max_wolff_terms new
--   where dels = not onlyDiag ? [] $ fromMaybe [] $ filter ((/=3) . (IntMap.! i3) . is'G) . toList . NestedFold <$> IntMap.lookup i3 (icgs'Ham ham)
--         new = mergeSortedWith (AbsF . snd)
--               [catMaybes [scale'GT dir <$> icomm'GT gT gT'
--                          | gT <- acommCandidates_sorted (fst gT') ham,
--                            not onlyDiag || IntMap.findWithDefault 0 i3 (is'G $ fst gT) /= 3 ]
--               | gT' <- _H0G] -- check sort?
-- 
-- -- onlyDiag makes no guarantees!
-- c4s_wolff_'Ham :: Bool -> Maybe Int -> F -> [G4_H0G] -> Ham -> Ham
-- c4s_wolff_'Ham onlyDiag max_wolff_terms dir g4_H0Gs ham_ =
--     if' onlyDiag diagFilter id $ foldl'_ (c4_wolff_'Ham onlyDiag max_wolff_terms dir) g4_H0Gs ham_
--   where diagFilter ham = ham -- foldl' (\ham' g -> all (==3) (is'G g) ? ham' $ fst $ deleteLookup'Ham g ham') ham
--                        -- $ NestedFold $ nlcgs'Ham ham -- profile this?
-- 
-- c4s_wolff'Ham :: Maybe Int -> F -> [G4_H0G] -> Ham -> Ham
-- c4s_wolff'Ham = c4s_wolff_'Ham False

-- Diag

-- TODO remove this
type TinyGT = (TinyG, F)

data TinyG = TinyG {
  hash'TinyG   :: !SigmaHash,
  size'TinyG   :: !Int,
  length'TinyG :: !(Maybe Int)}
  deriving (Show)

instance Eq TinyG where
  (==) = (==) `on` hash'TinyG

instance Ord TinyG where
  compare = compare `on` hash'TinyG

to'TinyG :: (Maybe Int) -> Sigma -> TinyG
to'TinyG l g = TinyG { hash'TinyG   = hash'G g,
                       size'TinyG   = size'G g,
                       length'TinyG = flip length'G g <$> l }

data MapTinyGT = MapTinyGT {
  gc'MapTinyGT :: !(Map TinyG F),
  l'MapTinyGT  :: !(Maybe Int)}
  deriving (Show)

empty'MapTinyGT :: Maybe Int -> MapTinyGT
empty'MapTinyGT = MapTinyGT Map.empty

-- to'MapTinyGT :: Maybe Int -> MapGT -> MapTinyGT
-- to'MapTinyGT l = flip MapTinyGT l . Map.mapKeys (to'TinyG l)

instance AddHash MapTinyGT (Sigma,F) where
  (MapTinyGT gc l) +# (g,c) = flip MapTinyGT l $ Map.alter (maybe (Just c) (+?c)) (to'TinyG l g) gc

data Diag = NoDiag | Diag'MapGT !MapGT | Diag'MapTinyGT !MapTinyGT
  deriving (Show)

instance AddHash Diag (Sigma,F) where
  NoDiag             +#  _ = NoDiag
  (Diag'MapGT     m) +# gc = Diag'MapGT     $ m +# gc
  (Diag'MapTinyGT m) +# gc = Diag'MapTinyGT $ m +# gc

-- RG

type G4_H0G = Either Sigma (F,Int,Int) -- Either g4 (rms of icomm H_0 Sigma/h_3^2, i3, j3)
-- if bosonic, the i3 bit and not the j3 bit is filled for the new stabilizer

data RG = RG {
  ls0'RG             :: ![Int],
  ham'RG             :: !Ham,
  diag'RG            :: !Diag,
  unusedIs'RG        :: !IntSet,
  g4_H0Gs'RG         :: ![G4_H0G],
  parity_stab'RG     :: !Bool,             -- RG found the parity stab
  offdiag_errors'RG  :: ![F],
  trash'RG           :: !(Maybe [[SigmaTerm]]),
  stab0'RG           :: ![SigmaTerm],      -- stabilizers in new basis
  stab1'RG           :: ![SigmaTerm],      -- stabilizers in current basis
  stab'RG            ::  Ham,              -- stabilizers in old basis
  max_rg_terms'RG    :: !(Maybe Int)}
  deriving (Show)

instance MySeq RG where
  mySeq rg = id -- deepseq rg
           . seq' (head . g4_H0Gs'RG)
           . seq' (head . offdiag_errors'RG)
           . seq' ((head<$>) . trash'RG)
           . seq' (head . stab0'RG)
           . seq' (head . stab1'RG)
    where seq' f = mySeq $ f rg

ls'RG :: RG -> [Int]
ls'RG = ls'Ham . ham'RG

n'RG :: RG -> Int
n'RG = n'Ham . ham'RG

g4s'RG :: RG -> [Sigma]
g4s'RG = lefts . g4_H0Gs'RG

wolff_errors'RG :: RG -> [F]
wolff_errors'RG = map fst3 . rights . g4_H0Gs'RG

init'RG :: [Int] -> Ham -> RG
init'RG ls0 ham = rg
  where rg = RG ls0 ham (Diag'MapGT Map.empty) (IntSet.fromDistinctAscList [0..(n'Ham ham - 1)]) [] False [] (Just []) [] [] (stabilizers rg) Nothing

-- g ~ sigma matrix, _G ~ Sigma, i ~ site index, h ~ energy coefficient
rgStep :: RG -> RG
rgStep rg@(RG _ ham1 diag unusedIs g4_H0Gs parity_stab offdiag_errors trash stab0 stab1 _ max_rg_terms)
  | IntSet.null unusedIs = rg
  | otherwise            = myForce rg'
  where
    _G1           :: [(SigmaTerm,SigmaTerm)] -- [(icomm g3' gT/h3', -gT)] where gT is in Sigma
    _G2, _G3, _G4 :: [SigmaTerm]
    g3        = fromMaybe g3_ $ (fst<$>) $ listToMaybe $ toDescList'Ham ham1
      where g3_ = fromList'G $ take 2 $ IntSet.toList unusedIs
    h3        = fromMaybe 0 $ Map.lookup g3 $ gc'Ham ham1
    i3_1      = IntSet.toList $ unusedIs `IntSet.intersection` is'G g3
    i3_2      = IntSet.toList $ unusedIs `IntSet.difference`   is'G g3
    (ij3,g3',g4s) | fermionicQ = case (i3_1, i3_2) of
                                      ([i3,j3], _) -> (Just (i3,j3), g3, [])
                                      (i3:_, j3:_) -> (Just (i3,j3), simp $ fromList'G [i3,j3], g4s_)
                                      (_, [])      -> (Nothing     , g3, [])
                                      _            -> error "rgStep ij3"
                  | otherwise  = let j3 = head i3_1
                                     i3 = odd j3 ? j3-1 $ j3+1
                                 in  (Just (i3, j3), simp $ fromList'G [i3], g4s_)
      where g4s_ = [fst $ fromJust $ icomm g3 g3']
            simp = not bifurcationQ ? id $ sigma . IntSet.union (is'G g3 `IntSet.difference` unusedIs) . is'G
    unusedIs'    = maybe unusedIs (\(i3,j3) -> unusedIs `IntSet.difference` IntSet.fromList [i3,j3]) ij3
    parity_stab' = (fermionicQ &&) $ assert (not $ isNothing ij3 && parity_stab) $ isNothing ij3 || parity_stab
    g4_H0Gs'     = maybe [] (\(i3,j3) -> [Right (rms $ map (snd . fst) _G1, i3, j3)]) ij3 ++ map Left (reverse g4s)
    
    isDiag :: SigmaTerm -> Bool
    isDiag (g,_) = not (any (flip IntSet.member unusedIs') $ toList'G g)
                || fermionicQ && parity_stab' && unusedIs' `IntSet.isSubsetOf` is'G g -- TODO just look at ground state, and then this simplifies
    
                  -- apply c4 transformation
    ham2           = c4s'Ham 1 g4s ham1
                  -- get g3' coefficient
    h3'            = fromMaybe 0 $ Map.lookup g3' $ gc'Ham ham2
                  -- find sigma matrices that overlap with g3'
                  -- split into diagonal matrices (diag') and anticommuting commutators (_G1)
    (diag',_G1)    = first (filter isDiag)
                   $ maybe (toList'Ham ham2, []) `flip` ij3
                   $ \(i3,j3) -> partitionEithers $ map (\gT -> maybe (Left gT) (Right . (,scale'GT (-1) gT)) $ icomm'GT (g3',recip h3') gT)
                              $ toList'MAS $ nearbySigmas' (nub $ map pos_i'G [i3,j3]) ham2 -- the minus is because we apply icomm twice
                  -- remove anticommuting matrices from ham
    ham3           = flip deleteSigmas'Ham ham2 $ map (fst . snd) _G1
                  -- [Sigma, Delta]
    _G_Delta       = [(icomm_sorted'Ham (g,1) ham3', c) | (g,c) <- map fst _G1]
                     where ham3' = delete'Ham g3' ham3
                  -- calc offdiag error
    offdiag_error  = maybe 0 (/abs h3') $ headMay $ map (abs . snd) $ filter (isJust . fst)
                   $ mergeSortedWith (AbsF . snd) $ map fst _G_Delta
                  -- remove diagonal matrices from ham
    ham4           = flip deleteSigmas'Ham ham3 $ map fst diag'
                  -- distribute _G1
    _G2            = catMaybes $ myParListChunk 64
                     [ icomm'GT gL gR
                     | gLRs@((gL,_):_) <- init $ tails _G1,
                       gR <- mapHead (scale'GT 0.5) $ map snd gLRs ]
                  -- extract diagonal terms
    (diag'',_G3)  = partition isDiag $ h3'==0 ? [] $ (fastSumQ ? id $ Map.toList . fromList'MapGT) _G2
                  -- keep only max_rg_terms terms
    (_G4, trash') = maybe (_G3,[]) (\max_terms -> splitAt max_terms $ sort'GT $ _G3) max_rg_terms
    -- TODO allow more terms by reducing to ground state
    
    rg' = rg {ham'RG            = ham4 +# _G4,
              diag'RG           = diag +# [diag',diag''],
              unusedIs'RG       = unusedIs',
              g4_H0Gs'RG        = g4_H0Gs' ++ g4_H0Gs,
              parity_stab'RG    = parity_stab',
              offdiag_errors'RG = offdiag_error : offdiag_errors,
              trash'RG          = Nothing, -- (trash':) <$> trash,
              stab0'RG          = (g3', h3'):stab0,
              stab1'RG          = (g3 , h3 ):stab1,
              stab'RG           = stabilizers rg'}

stabilizers :: RG -> Ham
stabilizers rg = c4s'Ham (-1) (g4s'RG rg) $ fromList'Ham (ls'RG rg) $ stab0'RG rg

runRG :: RG -> RG
runRG = until done rgStep
  where done rg | fermionicQ = parity_stab'RG rg && ((==2) $ IntSet.size $ unusedIs'RG rg)
                | otherwise  = IntSet.null $ unusedIs'RG rg

-- return: RMS of <g> over all eigenstates
-- Ham: the stabilizer Ham
rms'G :: Num a => Ham -> Sigma -> a
rms'G stab g0 = boole $ null $ acommSigmas g0 stab

-- entanglement entropy: arguments: list of region sizes and stabilizer Ham
ee1d :: Ls -> ([X] -> [Sigma]) -> [L] -> [X] -> [(L,X,Double,Double)]
ee1d ls cutStabs l0s x0s = map (\(l0,x0,ees) -> uncurry (l0,x0,,) $ meanError ees) $ ee1d_ ls cutStabs l0s x0s

ee1d_ :: Ls -> ([X] -> [Sigma]) -> [L] -> [X] -> [(L,X,[Double])]
ee1d_ ls cutStabs l0s x0s = [(l0, x0, [regionEE_1d ls cutStabs $ nub [(l0,x),(l0,x+x0)]
                                      | x <- [0..head ls-1]])
                            | l0 <- l0s,
                              x0 <- x0s]

-- entanglement entropy of the regions (l,x) -> [x..x+l-1]
regionEE_1d :: Ls -> ([X] -> [Sigma]) -> [(L,X)] -> Double
regionEE_1d ls cutStabs lxs_ = (0.5*) $ fromIntegral $ rankZ2 $ acommMat regionStabs
  where lsB = ls ++ (bosonicQ ? [2] $ [])
        nB  = product lsB
        lYB = product $ tail lsB
        lxs = map (second $ flip mod $ head ls) lxs_
        regionStabs = [sigma g'
                      | g <- map is'G $ cutStabs $ concatMap (\(l,x) -> [x,x+l]) lxs,
                        let g' = IntSet.unions $ map (selRegion g . both (*lYB)) lxs,
                        not (IntSet.null g') && g /= g']
        -- intersect g with region [i..i+l-1]
        selRegion :: IntSet -> (Int,Int) -> IntSet
        selRegion g (l,i) = (if i+l<nB then                fst . IntSet.split (i+l)
                                       else IntSet.union $ fst $ IntSet.split (i+l-nB) g)
                          $ snd $ IntSet.split (i-1) g
        acommMat :: [Sigma] -> IntMap IntSet
        acommMat gs  = foldl' f IntMap.empty gs
          where  gs' = fromList'Ham ls $ map (,1::F) gs
                 add i j = IntMap.insertWith IntSet.union i $ IntSet.singleton j
                 f mat0 g0 = foldl'_ (\j -> add i0 j . add j i0) iDs mat0
                   where iDs = map hash'G $ filter (\g -> hash'G g < i0 && acommQ g g0)
                             $ map fst $ acommCandidates g0 gs'
                         i0 = hash'G g0

-- stabilizers that may have been cut by [x..x+lx/2-1]
calcCutStabs :: Ham -> ([X] -> [Sigma])
calcCutStabs stab = \xs -> localStabs' xs ++ nonlocalStabs
  where
    ls     = ls'Ham stab
    lx     = head ls
    lx_2   = lx // 2
    modx   = flip mod lx
    modx_2 = flip mod lx_2
    
    localStabs' xs = Set.toList $ Set.unions $ map (IntMap.findWithDefault Set.empty & flip $ cutStabs0) $ nub $ map modx_2 xs
    x_i = head . xs_i'G ls
    localStabs, nonlocalStabs :: [Sigma]
    [localStabs, nonlocalStabs] = [toList $ NestedFold $ cgs stab | cgs <- [lcgs'Ham, nlcgs'Ham]]
    -- local stabilizers cut by [x..x+lx/2-1] where 0 <= i < lx/2 due to symmetry
    cutStabs0 :: IntMap (Set Sigma)
    cutStabs0 = IntMap.fromListWith Set.union $ concatMap f localStabs
    f :: Sigma -> [(Int,Set Sigma)]
    f g = map (,Set.singleton g) $ cutRegions $ intUnion $ map x_i $ toList'G g
    cutRegions :: [Int] -> [Int]
    cutRegions xs = concatMap region0 $ zipExact xs (rotateLeft 1 xs)
    region0 (x,x') | dx <= lx_2 = map modx_2 [x+1 .. x+dx]
                   | otherwise  = region0 (x',x)
      where dx = modx $ x' - x

type Rands = [F]
randoms :: Int -> Rands
randoms seed = map toF $ Random.randoms $ Random.mkStdGen seed

type X  = Int
type Xs = [X]
type L  = Int
type Ls = [L]

pos_xs :: Ls -> Xs -> Pos
pos_xs ls xs = foldl' (\i (l,x) -> i*l + mod x l) 0 $ zipExact ls xs

pos_xs' :: Ls -> Xs -> Pos
pos_xs' ls xs = pos_xs ls $ xs ++ replicate (length ls - length xs) 0

xs_pos :: Ls -> Pos -> Xs
xs_pos ls i0 = map snd $ init $ scanr (\l (i,_) -> divMod i l) (i0,0) ls

-- site -> randoms -> (SigmaTerms, unused_randoms)
type ModelGen = Xs -> Rands -> ([SigmaTerm], Rands)

init_generic'Ham :: Int -> (Ls,ModelGen) -> Ham
init_generic'Ham seed (ls,model) = fromList'Ham ls $ filter ((/=0) . snd) $ concat
                                $ flip State.evalState (randoms seed) $
                                  mapM (State.state . model) $ mapM (\l -> [0..l-1]) ls

data Model =
#ifdef BOSONIC
  Ising | MajSquare | MajSquareOpen
#else
  MajSquareF
#endif
  deriving (Eq, Show, Read, Enum)

#ifdef FERMIONIC
type RawSigmaTermF  = ([Xs],F)
basic_genF :: Ls -> (Xs -> Rands -> ([RawSigmaTermF], Rands)) -> (Ls,ModelGen)
basic_genF ls gen = (ls, (\(gs,rs) -> ([fromList'GT (map (pos_xs ls) g,c) | (g,c) <- gs], rs)) .* gen)
#else
type RawSigmaTermB = ([(Xs,Int)],F)
basic_genB :: Ls -> (Xs -> Rands -> ([RawSigmaTermB], Rands)) -> ([Int],ModelGen)
basic_genB ls gen = (ls, (\(gs,rs) -> ([fromListB'GT (map (first $ pos_xs ls) g,c) | (g,c) <- gs], rs)) .* gen)
#endif

model_gen :: Model -> Ls -> Rands -> (Ls,ModelGen)
#ifdef BOSONIC
model_gen Ising ls [j,k,h] = basic_genB ls gen
  where (kj,kh) = (3,1)
        gen [x] (rj:rk:rh:rs) =
          ([ ([([x],kj),([x+1],kj)],j*rj), ([([x],kh),([x+1],kh)],k*rk), ([([x],kh)],h*rh) ], rs)
        gen [x,y] (rjx:rjy:rkx:rky:rh:rs) =
          ([ ([([x,y],kj),([x+1,y  ],kj)],j*rjx), ([([x,y],kh),([x+1,y  ],kh)],k*rkx), ([([x,y],kh)],h*rh),
             ([([x,y],kj),([x  ,y+1],kj)],j*rjy), ([([x,y],kh),([x  ,y+1],kh)],k*rky) ], rs)
        gen _ _ = error "Ising"
model_gen model ls [ly_] | model `elem` [MajSquare, MajSquareOpen] = basic_genB (ls++[ly0]) gen
  where ly  = round $ toDouble ly_
        ly0 = (ly-1)//2
        oQ  = boole $ model == MajSquareOpen
        gg x y | y >= ly-oQ = error "gg"
               | y == ly-1  = ([x,0],2) : [([x,y0'],3) | y0' <- [1..ly0-1]]
               | y == ly-2  = [([x,y0],1)]
               | z == 0     = [([x,y0],3)]
               | otherwise  = [([x,y0],1),([x,y0+1],1)]
          where (y0,z) = divMod y 2
        gen [x,0] rs0 = ([(gg x y ++ gg (x+1) y,r) | (y,r) <- zip [0..] rs], rs')
          where (rs,rs') = splitAt (ly-oQ) rs0
        gen [_,_] rs = ([], rs)
        gen _ _ = error "MajSquare"
#endif
model_gen _ _ _ = error "model_gen"

model_gen_apply_gamma :: Double -> (Ls,ModelGen) -> (Ls,ModelGen)
model_gen_apply_gamma gamma (ls,gen) = (ls, first (map $ second $ gamma==0 ? (boole . (/=0)) $ (`powF` gamma)) .* gen)

-- [bins (upper bound)] -> [(bin,x)] -> [(bin,xs)]
generic_histo_ :: [Int] -> [(Int,a)] -> [(Int,[a])]
generic_histo_ xs = IntMap.toList . foldl'
                      (\hist (x,y) -> (flip $ IntMap.alter $ Just . (y:) . fromJust) hist
                                      $ fst $ fromJust $ IntMap.lookupGE x hist)
                      (IntMap.fromListWith undefined $ map (,[]) xs)

-- float type

type_F    :: String
toF       :: Double -> F
toDouble  :: F -> Double
powF      :: F -> Double -> F -- preserves sign of base
expF      :: Double -> F
logF      :: F -> Double
succIEEEF :: F -> F

-- type F    = Double
-- type_F    = "Double"
-- toF       = id
-- toDouble  = id
-- powF      = \x y -> signum x * (abs x ** y)
-- expF      = exp
-- logF      = log
-- succIEEEF = succIEEE

type F    = LogFloat
type_F    = "LogFloat"
toF       = fromDouble'LF
toDouble  = toDouble'LF
powF      = pow'LF
expF      = exp'LF
logF      = log'LF
succIEEEF = succIEEE'LF

-- main

fermionicQ, bosonicQ :: Bool
#ifdef FERMIONIC
fermionicQ = True
#else
fermionicQ = False
#endif
bosonicQ = not fermionicQ

bifurcationQ :: Bool
bifurcationQ = True -- TODO implement false

prettyPrint :: Bool
prettyPrint = False

fastSumQ :: Bool
fastSumQ = False

parallelQ :: Bool
parallelQ = False

main :: IO ()
main = do
  startTime <- Clock.getTime Clock.Monotonic
  
  let small_lsQ               = False
      max_rg_terms_default    = "32"
    --max_wolff_terms_default = "4"
  
  args <- getArgs
  when (length args < 5) $ do
    putStrLn $ "usage: SBRG random-seed model [" ++ if' small_lsQ "" "ln2 " ++ "system lengths] [coupling constants] gamma"
            ++ " {max RG terms = " ++ max_rg_terms_default ++ "}" -- {max wolff terms = " ++ max_wolff_terms_default ++ "}"
    putStr   $ "available models: "
    print    $ enumFrom $ (toEnum 0 :: Model)
    putStrLn $ "example: SBRG 0 Ising [6] [1,1,1] 1"
    exitFailure
  
  (seed,model,ln2_ls,couplings,gamma,max_rg_terms_) <- getArgs6 [max_rg_terms_default]
    :: IO (Int, Model, [Int], [F], Double, Int)
  
--   (seed,model,ln2_ls,couplings,gamma,max_rg_terms_,max_wolff_terms_) <- getArgs7 [max_rg_terms_default, max_wolff_terms_default]
--     :: IO (Int, Model, [Int], [F], Double, Int, Int)
  
  let alterSeedQ     = True
      allStabsQ      = False
      calc_EEQ       = True
    --calc_aCorrQ    = False
    --calc_aCorr'Q   = False
      detailedQ      = False
      cut_powQ       = True
      keep_diagQ     = length ln2_ls <= 1
      full_diagQ     = False
  
  let max_rg_terms    = justIf' (>=0) max_rg_terms_
    --max_wolff_terms = justIf' (>=0) max_wolff_terms_
      ls0      = small_lsQ ? ln2_ls $ map (2^) ln2_ls
      dim      = length ls0
      ls       = ls'RG rg0
      n        = n'RG rg0
      l_1d     = dim == 1 ? Just n $ Nothing
      seed'    = not alterSeedQ ? hash seed $ hash $ show (seed, model, ln2_ls, couplings, gamma)
      rg0      = init'RG ls0 $ init_generic'Ham seed' $ model_gen_apply_gamma gamma $ model_gen model ls0 couplings
      rg       = runRG $ rg0 { diag'RG            = not keep_diagQ
                                                    ? NoDiag
                                                    $ full_diagQ ? diag'RG rg0
                                                    $ Diag'MapTinyGT $ empty'MapTinyGT l_1d,
                               trash'RG           = Nothing,
                               max_rg_terms'RG    = max_rg_terms }
      xs_      = (small_lsQ ? id $ filter isPow2) [1..head ls//2]
  
  unless (0 < n) $ undefined
  
  putStr   "version:            "; putStrLn "190617.0" -- year month day . minor
  putStr   "warnings:           "; print $ catMaybes [justIf fastSumQ "fastSum"]
  putStr   "model:              "; print $ show model
  putStr   "Ls:                 "; print ls0
  putStr   "couplings:          "; print $ (read $ args!!3 :: [Double]) -- print couplings
  putStr   "Γ:                  "; print gamma
  putStr   "seed:               "; print seed
  putStr   "seed':              "; print seed'
  putStr   "max RG terms:       "; print max_rg_terms
--when calc_aCorr'Q $ do
--  putStr "max Anderson terms: "; print max_wolff_terms
  putStr   "float type:         "; print type_F
  
  when detailedQ $ do
    putStr "Hamiltonian: "
    print $ ham'RG rg0
  
  let n_missing = length $ takeWhile ((==0) . snd) $ stab0'RG rg
      cut_pow2 = reverse . if' (cut_powQ && not small_lsQ) (filter $ isPow2 . fst) id . zip [1..]
      ordered_stab = map (c4s (-1) (g4s'RG rg)) $ stab0'RG rg
  
  putStr "missing stabilizers: "
  print $ n_missing
--mapM_ print $ map (toLists'GT ls) $ take 6 ordered_stab
  
  putStr "Wolff errors: "
  print $ cut_pow2 $ wolff_errors'RG rg
  
  putStr "offdiag errors: "
  print $ cut_pow2 $ offdiag_errors'RG rg
  
  putStr "stabilizer energies: "
  print $ cut_pow2 $ map snd $ stab0'RG rg
  
  putStr "stabilizer sizes: "
  print $ cut_pow2 $ map (size'G . fst) $ ordered_stab
  
  when (dim == 1) $ do
    putStr "stabilizer lengths: "
    print $ cut_pow2 $ map (length'G (head ls) . fst) $ ordered_stab
    
    putStr "stabilizer length and energy: "
    print $ map (first $ length'G $ head ls) $ ordered_stab
  
  when detailedQ $ do
    putStr "stabilizers: "
    print $ stab'RG rg
    
    putStr "stabilizers0: "
    print $ stab0'RG rg
    
    putStr "stabilizers1: "
    print $ stab1'RG rg
    
    case diag'RG rg of
      Diag'MapGT diag -> do putStr "effective Hamiltonian: "
                            print $ diag
      _ -> return ()
  
  do
    let log_ns      = reverse $ takeWhile (/=0) $ iterate (flip div 2) n
        small_ns n_ = n<=n_ ? [1..n] $ [1..n_]++[n]
        
        -- [(bin upper bound, #, RSS of coefficients, sum of logs, max)]
        generic_histo :: [Int] -> (TinyG -> Int) -> [TinyGT] -> [(Int,Int,F,Double,F)]
        generic_histo ns f = map (\(n_,cs) -> (n_,length cs,rss cs, sum $ map (logF . abs) $ filter (/=0) cs, maximum $ map abs $ 0:cs)) . generic_histo_ ns . map (first f)
        
        length_histo :: [Int] -> [TinyGT] -> [(Int,Int,F,Double,F)]
        length_histo ns = generic_histo ns (fromJust . length'TinyG)
        
        all_histos :: String -> ([Int],[Int]) -> [Int] -> [TinyGT] -> IO ()
        all_histos name (nsS,nsS') nsL gcs = do
          putStr $ name ++ " size histo: "
          print $ generic_histo nsS size'TinyG gcs
          
          when (dim == 1) $ do
            putStr $ name ++ " length histo: "
            print $ length_histo nsL gcs
            
            putStr $ name ++ " size-length histo: "
            print $ map (second $ length_histo nsL) $ generic_histo_ nsS' $ map (\gT@(g,_) -> (size'TinyG g, gT)) gcs
        
        toTinys = map (first $ to'TinyG l_1d) . Map.toList
    
    putStr "small stabilizers: "
    print $ map (first $ size'G)
          $ (allStabsQ ? id $ uncurry (++) . second (take 20) . span ((==0) . snd))
          $ sortWith (abs . snd) $ toList'Ham $ stab'RG rg
    
    all_histos "stabilizer"      (log_ns     , log_ns     ) log_ns         $ toTinys $ gc'Ham $ stab'RG rg
    all_histos "diag"            (small_ns 32, small_ns 16) log_ns & mapM_ $
      case diag'RG rg of
           NoDiag           -> Nothing
           Diag'MapGT     m -> Just $ toTinys m
           Diag'MapTinyGT m -> Just $ Map.toList $ gc'MapTinyGT m
  
  let cutStabs = calcCutStabs $ stab'RG rg
  
  when calc_EEQ $ do
    let mapMeanError = map (\(l0,x0,ees) -> uncurry (l0,x0,,) $ meanError ees)
        entanglement_data = ee1d_ ls cutStabs xs_ (small_lsQ ? [0] $ 0:xs_)
        entanglement_map = Map.fromListWith undefined $ map (\(l,x,es) -> ((l,x),es)) entanglement_data
        lrmi_data = mapMeanError $ flip mapMaybe entanglement_data $
                      \(l,x,es) -> let es0 = entanglement_map Map.! (l,0) in
                                       justIf (x>=l) (l, x, zipWith3 (\e0 e0' e -> e0 + e0' - e) es0 (rotateLeft x es0) es)
    
    putStr "entanglement entropy: " -- [(region size, region separation, entanglement entropy, error)]
    print $ mapMeanError entanglement_data
    
    putStr "long range mutual information: "
    print $ lrmi_data
  
--   let aCorr  = uncurry3 anderson_corr  corr_z_xs_ggs $ stab'RG rg
--       aCorr' = uncurry3 anderson_corr' corr_z_xs_ggs           rg
-- 
--   when calc_aCorrQ $ do
--    putStr "anderson correlator:  " -- [(distance, [xyz -> (correlator,error)])]
--    print $ aCorr
-- 
--   when calc_aCorr'Q $ do
--    putStr "anderson correlator': " -- [(distance, [xyz -> (correlator,error)])]
--    max_wolff_terms_==0 ? print aCorr $ print aCorr'
  
  putStr "CPU time:  "
  cpu_time <- CPUTime.getCPUTime
  print $ (1e-12::Double) * fromInteger cpu_time
  
  putStr "WALL time: "
  endTime <- Clock.getTime Clock.Monotonic
  print $ (1e-9::Double) * (fromInteger $ on (-) Clock.toNanoSecs endTime startTime)
  
--   gcStatsQ <- GHC.getRTSStatsEnabled
--   when gcStatsQ $ do
--     putStr "RAM used (MB): "
--     gcStats <- GHC.getRTSStats
--     print $ GHC.gcStatsPeakMegabytesAllocated gcStats

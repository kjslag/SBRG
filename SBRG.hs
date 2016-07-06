-- author: Kevin Slagle
-- github.com/kjslag/SBRG

-- ghci -fbreak-on-error
-- :set -fbreak-on-error
-- :set args 1 XYZ [4] [1,1,1] 1
-- :trace main

{-# LANGUAGE TupleSections, BangPatterns, MagicHash, MultiParamTypeClasses, FlexibleInstances, GeneralizedNewtypeDeriving, DeriveFunctor, DeriveFoldable #-} -- OverloadedLists
-- :set -XTupleSections

{-# OPTIONS -Wall -Wno-unused-top-binds -fno-warn-unused-imports -Wno-orphans -O -optc-O2 -optc-march=native -optc-mfpmath=sse #-}
-- -rtsopts -prof -fprof-auto        -ddump-simpl -threaded
-- +RTS -xc -s -p                    -N4
-- +RTS -hy && hp2ps -c SBRG.hp && okular SBRG.ps

import GHC.Prim (reallyUnsafePtrEquality#)
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
import Data.Hashable as Hashable
import Data.List
import Data.Maybe
import Data.Monoid
import Data.NumInstances.Tuple
import Data.Ord
import Data.Tuple
import Debug.Trace
import Numeric.IEEE (nan,infinity)
import Safe
import Safe.Exact
import System.Environment
import System.CPUTime
import System.Exit (exitSuccess, exitFailure)

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
import qualified Data.Map.Strict as Map hiding (fromList, insert, delete, adjust, adjustWithKey, update, updateWithKey) -- use alter instead

-- debug

error_ :: a
error_ = error "compile with '-rtsopts -prof -fprof-auto' and run with '+RTS -xc' for a stack trace"

assert :: Bool -> a -> a
assert True  = id
assert False = error "assert"

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
-- fromLeft _        = error_

-- fromRight :: Either a b -> b
-- fromRight (Right y) = y
-- fromRight _         = error_

infixl 7 //
(//) :: Integral a => a -> a -> a
x // y | (q,0) <- divMod x y = q
       | otherwise           = error_

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
mapHead _ _      = error_

foldl'_ :: Foldable t => (a -> b -> b) -> t a -> b -> b
foldl'_ = flip . foldl' . flip

partitions :: Int -> [a] -> [[a]]
partitions _ [] = []
partitions n xs = uncurry (:) $ second (partitions n) $ splitAt n xs

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

-- minimumOn :: Ord b => (a -> b) -> [a] -> a
-- minimumOn f xs = snd $ minimumBy (comparing fst) $ map (\x -> (f x, x)) xs

-- maximumOn :: Ord b => (a -> b) -> [a] -> a
-- maximumOn f xs = snd $ maximumBy (comparing fst) $ map (\x -> (f x, x)) xs

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
mean = (\(n,x) -> x/fromIntegral n) . sum . map (\x -> (1::Int,x))

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

-- Vector

newtype V2 a = V2 (a,a  ) deriving (Functor, Foldable, Num, Fractional, Floating, MySeq)
newtype V3 a = V3 (a,a,a) deriving (Functor, Foldable, Num, Fractional, Floating, MySeq)

instance Show a => Show (V2 a) where  show = show . toList
instance Show a => Show (V3 a) where  show = show . toList

infixl 7 *^
(*^) :: (Functor f, Num a) => a -> f a -> f a
(*^) a = fmap (a*)

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
  mySeq (x,y) = mySeq x . mySeq y

instance (MySeq a, MySeq b, MySeq c) => MySeq (a,b,c) where
  mySeq (x,y,z) = mySeq x . mySeq y . mySeq z

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
-- pow'LF _ _ = error_

log'LF :: LogFloat -> Double
log'LF (LogFloat True y) = y
log'LF _                 = error_

isNaN'LF :: LogFloat -> Bool
isNaN'LF (LogFloat _ y) = isNaN y
  
instance MySeq LogFloat

instance Show LogFloat where
  showsPrec n z@(LogFloat s y) str
    | abs y > 600 && not (isInfinite y || isNaN y)
      = if' s "" "-" ++ (abs y > recip epsilon
                        ? "10^" ++ shows (y/ln10) str
                        $ shows (exp $ ln10*y') ('e' : shows (e::Integer) str) )
    | otherwise = showsPrec n (toDouble'LF z) str
    where (e,y')  = properFraction $ y / ln10
          ln10    = log 10

instance Read LogFloat where
  readsPrec = map (first fromDouble'LF) .* readsPrec

instance Eq LogFloat where
  (==) = (==EQ) .* compare

instance Ord LogFloat where
  compare (LogFloat s1 y1) (LogFloat s2 y2)
    |    y1 == -infinity
      && y2 == -infinity   = EQ
    | isNaN y1 || isNaN y2 = error_ -- GT -- http://stackoverflow.com/a/6399798
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
  sqrt _                 = error_
  pi    = error_
  exp   = error_
  log   = error_
  sin   = error_
  cos   = error_
  asin  = error_
  acos  = error_
  atan  = error_
  sinh  = error_
  cosh  = error_
  asinh = error_
  acosh = error_
  atanh = error_

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
rankZ2' = rankZ2 . IntMap.fromListWith error_ . zip [0..] . map (IntSet.fromList . map fst . filter snd . zip [0..])

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
  AbsF x `compare` AbsF y = compare (abs y) (abs x)

-- Sigma

type SigmaHash = Int

data Sigma = Sigma {
  ik'G   :: !(IntMap Int),
  iD'G   :: !Int,
  meta'G :: !Meta'G,
  hash'G ::  SigmaHash }

sigma :: Int -> IntMap Int -> Sigma
sigma iD ik = g
  where g = Sigma ik iD default_meta'G $ calc_hash'G g

data IntPair = IntPair !Int !Int

calc_hash'G :: Sigma -> SigmaHash
calc_hash'G g = (\(IntPair s x) -> hashWithSalt s x) $ IntMap.foldlWithKey' f (IntPair (hash $ iD'G g) (0::Int)) $ ik'G g
  where f (IntPair s n) k x = IntPair (hashWithSalt s k `hashWithSalt` x) (n + 1)

-- calc_hash'G :: Sigma -> SigmaHash
-- calc_hash'G g = (\(s,x) -> hashWithSalt s x) $ IntMap.foldlWithKey' f (hash $ iD'G g, 0::Int) $ ik'G g
--   where f (!s,!n) k x = (hashWithSalt s k `hashWithSalt` x, n + 1)

-- http://hackage.haskell.org/package/hashable-1.2.4.0/docs/src/Data.Hashable.Class.html#line-452
instance Hashable Sigma where
  hashWithSalt salt = hashWithSalt salt . hash'G
  hash = hash'G

set_iD'G :: Int -> Sigma -> Sigma
set_iD'G iD g = g'
  where g' = g { iD'G = iD, hash'G = calc_hash'G g' }

set_meta'G :: Meta'G -> Sigma -> Sigma
set_meta'G meta g = g { meta'G = meta }

instance MySeq Sigma

instance Eq Sigma where
  Sigma g1 iD1 _ hash1 == Sigma g2 iD2 _ hash2 = hash1==hash2 && iD1==iD2 && (ptrEquality g1 g2 || g1==g2)

instance Ord Sigma where
  Sigma g1 iD1 _ hash1 `compare` Sigma g2 iD2 _ hash2 =
      compare hash1 hash2 <> compare iD1 iD2 <> (ptrEquality g1 g2 ? EQ $ compare' g1 g2)
    where compare' x y = compare x y

instance Show Sigma where
  showsPrec n g = showsPrec n $ IntMap.toList $ ik'G g

type SigmaTerm = (Sigma, F)

check'Sigma :: Sigma -> Bool
check'Sigma g = all (\k -> 1<=k && k<=3) $ IntMap.elems $ ik'G g

toList'GT :: SigmaTerm -> ([(Int,Int)],F)
toList'GT = first $ IntMap.toList . ik'G

toLists'G :: [Int] -> Sigma -> [([Int],Int)]
toLists'G ls = map (first $ xs_i ls) . IntMap.toList . ik'G

toLists'GT :: [Int] -> SigmaTerm -> ([([Int],Int)],F)
toLists'GT ls = first $ toLists'G ls

fromList'GT :: Int -> ([(Int,Int)],F) -> SigmaTerm
fromList'GT iD = first $ \g -> sigma iD $ IntMap.fromListWith (error $ "fromList'GT: " ++ show g) g

show'GTs :: [SigmaTerm] -> String
show'GTs = if' prettyPrint (concat . intersperse " + " . map (\(g,c) -> show c ++ " σ" ++ show g)) show

sort'GT :: [(a,F)] -> [(a,F)]
sort'GT = sortWith (AbsF . snd)

scale'GT :: F -> SigmaTerm -> SigmaTerm
scale'GT x (g,c) = (g,x*c)

length'G :: Int -> Sigma -> Int -- TODO d>1
length'G l = (l-) . maximum . modDifferences l . IntMap.keys . ik'G

size'G :: Sigma -> Int
size'G = IntMap.size . ik'G

acommQ :: Sigma -> Sigma -> Bool
acommQ g1 g2 = foldl_ xor False $ (intersectionWith' neq' `on` ik'G) g1 g2
  where foldl_ f x y = IntMap.foldl' f x y
        intersectionWith' f x y = IntMap.intersectionWith f x y
        neq' x y = x /= y

-- acommQ = acommQ_ `on` ik'G
--   where acommQ_ g1 g2 =
--           case splitRoot1 g1 of
--                [l1,r1] -> case splitRoot2 g2 of
--                                [l2,r2] -> acommQ_ l1 l2 `xor` acommQ_ r1 r2 `xor` acommQ' l1 r2 `xor` acommQ' l2 r1
--                                [v2]    -> f v2 g1
--                                []      -> False
--                                _       -> error_
--                [v1]    -> f v1 g2
--                []      -> False
--                _       -> error_
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
--multSigma1 k 0 = ( 0,k)
--multSigma1 0 k = ( 0,k)
multSigma1 _ _ = error_

multSigma :: Sigma -> Sigma -> (Int,Sigma)
multSigma (Sigma g1 iD1 meta1 _) (Sigma g2 iD2 meta2 _) = (n, g' {meta'G = merge_meta'G meta1 meta2})
  where n  = IntMap.foldl' (+) 0 $ intersectionWith' (fst .* multSigma1) g1 g2 -- a IntMap.foldIntersectionWith would be faster
        g' = sigma iD $ IntMap.mergeWithKey (\_ k1 k2 -> justIf' (/=0) $ snd $ multSigma1 k1 k2) id id g1 g2
        iD | iD2==0 || iD1==iD2 = iD1
           | iD1==0             = iD2
           | otherwise          = error_
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
                   _ -> error_
  where (n,g) = multSigma g1 g2

-- {a,b}/2
acomm :: Sigma -> Sigma -> Maybe SigmaTerm
acomm g1 g2 = case mod n 4 of
                   0 -> Just (g, 1)
                   2 -> Just (g,-1)
                   1 -> Nothing
                   3 -> Nothing
                   _ -> error_
  where (n,g) = multSigma g1 g2

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

toLists'MapGT :: MapGT -> [([(Int,Int)],F)]
toLists'MapGT = map toList'GT . Map.toList

fromList'MapGT :: [SigmaTerm] -> MapGT
fromList'MapGT = Map.fromListWith (+)

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
data Ham = Ham {
  gc'Ham    :: !MapGT,                 -- Hamiltonian:  sigma matrix  -> coefficient
  lcgs'Ham  :: !MapAbsSigmas,          -- |coefficient| ->     local sigma matrices
  nlcgs'Ham :: !MapAbsSigmas,          -- |coefficient| -> non-local sigma matrices
  icgs'Ham  :: !(IntMap MapAbsSigmas), -- local sigmas: site index -> |coefficient| -> local sigma matrices
  ls'Ham    :: ![Int] }                -- system lengths
  deriving (Show)

n'Ham :: Ham -> Int
n'Ham = product . ls'Ham

instance MySeq Ham

-- instance Show Ham where
--   show = show'GTs . Map.toList . gc'Ham

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
toDescList'Ham ham = toSortedList'MASs [lcgs'Ham ham, nlcgs'Ham ham]

fromList'Ham :: [Int] -> [SigmaTerm] -> Ham
fromList'Ham ls = (zero'Ham ls +#)

fromLists'Ham :: [Int] -> Int -> [([(Int,Int)],F)] -> Ham
fromLists'Ham ls iD = fromList'Ham ls . map (fromList'GT iD)

null'Ham :: Ham -> Bool
null'Ham = Map.null . gc'Ham

insert'Ham :: SigmaTerm -> Ham -> Ham
insert'Ham gT@(!g,!c) ham@(Ham gc lcgs nlcgs icgs ls)
-- | c == 0       = ham -- this line would break stab'RG
  | isNothing c_ =
    Ham (Map.insertWith error_ g c gc)
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
        icgs'         = IntMap.differenceWith (\cgs _ -> justIf' (not . Map.null) $ delete'MAS gT cgs) icgs $ ik'G g

deleteSigmas'Ham :: Foldable t => t Sigma -> Ham -> Ham
deleteSigmas'Ham = foldl'_ delete'Ham

acommSigmas :: Sigma -> Ham -> [SigmaTerm]
acommSigmas g = filter (acommQ g . fst) . acommCandidates g

acommCandidates :: Sigma -> Ham -> [SigmaTerm]
acommCandidates = (\(nlgs:lgs) -> toList'MAS nlgs ++ toSortedList'MASs lgs) .* acommCandidates_

acommCandidates_sorted :: Sigma -> Ham -> [SigmaTerm]
acommCandidates_sorted = toSortedList'MASs .* acommCandidates_

acommCandidates_ :: Sigma -> Ham -> [MapAbsSigmas]
acommCandidates_ g ham = nlcgs'Ham ham : IntMap.elems localSigmas
  where localSigmas = IntMap.intersection (icgs'Ham ham) $ ik'G g

nearbySigmas :: Int -> Ham -> MapAbsSigmas
nearbySigmas i ham = nearbySigmas' [i] ham

nearbySigmas' :: [Int] -> Ham -> MapAbsSigmas
nearbySigmas' is ham = nlcgs'Ham ham +# [IntMap.lookup i $ icgs'Ham ham | i <- is]

-- the Maybe SigmaTerm has a +/- 1 coefficient
icomm_sorted'Ham :: SigmaTerm -> Ham -> [(Maybe SigmaTerm, F)]
icomm_sorted'Ham (g,c) ham = map (\(g',c') -> (icomm g g', c*c')) $ acommCandidates_sorted g ham

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
                                       Just (g',c') -> second (\c -> (g',dir*c*c'):gTs) $ deleteLookup'Ham g ham0

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
c4_wolff_'Ham True     _               dir  _ _ | dir /= 1       = error_
c4_wolff_'Ham _        _               dir (Left    g4     ) ham = c4'Ham dir g4 ham
c4_wolff_'Ham onlyDiag max_wolff_terms dir (Right (_H0G,i3)) ham = deleteSigmas'Ham dels ham
                                                                 +# maybe id take max_wolff_terms new
  where dels = not onlyDiag ? [] $ fromMaybe [] $ filter ((/=3) . (IntMap.! i3) . ik'G) . toList . NestedFold <$> IntMap.lookup i3 (icgs'Ham ham)
        new = mergeSortedWith (AbsF . snd)
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

-- Diag

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

type G4_H0G = Either Sigma ([SigmaTerm],Int) -- Either g4 (icomm H_0 Sigma/h_3^2, i3)

data RG = RG {
  model'RG           :: !Model,
  ls0'RG             :: ![Int],
  ham0'RG            :: !Ham,              -- original Ham in old basis
  c4_ham0'RG         ::  Ham,              -- original ham in new basis
  ham'RG             :: !Ham,
  diag'RG            :: !Diag,
  unusedIs'RG        :: !IntSet,
  g4_H0Gs'RG         :: ![G4_H0G],
  offdiag_errors'RG  :: ![F],
  trash'RG           :: !(Maybe [[SigmaTerm]]),
  stab0'RG           :: ![SigmaTerm],      -- stabilizers in new basis
  stab1'RG           :: ![SigmaTerm],      -- stabilizers in current basis
  stab'RG            ::  Ham,              -- stabilizers in old basis
  meta'RG            :: !Meta'RG,
  max_rg_terms'RG    :: !(Maybe Int),
  max_wolff_terms'RG :: !(Maybe Int)}
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

i3s'RG :: RG -> [Int]
i3s'RG = map snd . rights . g4_H0Gs'RG

wolff_errors'RG :: RG -> [F]
wolff_errors'RG = map (rss . map snd . fst) . rights . g4_H0Gs'RG

init'RG :: Model -> [Int] -> Ham -> RG
init'RG model ls0 ham = rg
  where rg  = RG model ls0 ham ham ham (Diag'MapGT Map.empty) (IntSet.fromDistinctAscList [0..(n'Ham ham - 1)]) [] [] (Just []) [] [] (stabilizers rg) (init_meta'RG rg) Nothing Nothing

-- g ~ sigma matrix, _G ~ Sigma, i ~ site index, h ~ energy coefficient
rgStep :: RG -> RG
rgStep rg@(RG model _ _ c4_ham0 ham1 diag unusedIs g4_H0Gs offdiag_errors trash stab0 stab1 _ _ max_rg_terms _)
  | IntSet.null unusedIs = rg
  | otherwise            = myForce $ update_meta'RG rg rg'
  where
    _G1           :: [(SigmaTerm,SigmaTerm)] -- [(icomm g3' gT/h3', -gT)] where gT is in Sigma
    _G2, _G3, _G4 :: [SigmaTerm]
    g3        = fromMaybe g3__ g3_
      where g3_  = (fst<$>) $ listToMaybe $ toDescList'Ham ham1
            g3__ = case model of
                        ToricCode -> head $ sortOn calc_size $ map (sigma 0 . IntMap.fromListWith error_ . filter ((/=0) . snd))
                                          $ tail $ traverse (\i -> map (i,) [0..3]) $ IntSet.toList unusedIs
                        _ -> (!!1) $ sortOn calc_size [sigma 0 $ IntMap.fromSet (const k) unusedIs | k <- [1..3]]
            calc_size = size'G . fst . c4s (-1) (g4s'RG rg) . (,0)
    h3        = fromMaybe 0 $ Map.lookup g3 $ gc'Ham ham1
  --unusedIs_ = IntMap.fromSet (const ()) unusedIs
  --i3        = snd $ the $ until (null . drop 1) (\is -> let is' = filter (even . fst) is in map (first (flip div 2)) $ null is' ? is $ is') $ map (\i -> (i,i))
    i3        = fst $ maximumBy (comparing snd) $ (\is -> zipExact is $ modDifferences (n'Ham ham1) is)
              $ IntSet.toList $ IntSet.intersection (IntMap.keysSet $ ik'G g3) unusedIs
    unusedIs' = IntSet.delete i3 unusedIs
    g3'       = set_meta'G (meta'G g3) $ sigma 0 $ IntMap.singleton i3 3
  --g3'       = sigma 0 $ IntMap.insertWith error_ i3 3 $ IntMap.difference (ik'G g3) unusedIs_
    g4s_      = map g4s_meta'G $ find_G4s g3 g3'
    c4_ham0'  = c4s'Ham 1 g4s_ c4_ham0
    g4_H0Gs'  = Right (sort'GT $ map fst _G1, i3) : map Left (reverse g4s_)
    
                    -- apply c4 transformation
    ham2             = c4s'Ham 1 g4s_ ham1
                    -- get g3' coefficient
    h3'              = fromMaybe 0 $ Map.lookup g3' $ gc'Ham ham2
                    -- find sigma matrices that overlap with g3'
                    -- split into diagonal matrices (diag') and anticommuting commutators (_G1)
    (diag',_G1)      = first (filter isDiag) $ partitionEithers
                     $ map (\gT -> maybe (Left gT) (Right . (,scale'GT (-1) gT)) $ icomm'GT (g3',recip h3') gT)
                     $ toList'MAS $ nearbySigmas i3 ham2              -- the minus is because we apply icomm twice
                    -- remove anticommuting matrices from ham
    ham3             = flip deleteSigmas'Ham ham2 $ map (fst . snd) _G1
                    -- [Sigma, Delta]
    _G_Delta         = [(icomm_sorted'Ham (g,1) ham3', c) | (g,c) <- map fst _G1]
                       where ham3' = delete'Ham g3' ham3
                    -- calc offdiag error
    offdiag_error    = maybe 0 (/abs h3') $ headMay $ map (abs . snd) $ filter (isJust . fst)
                     $ mergeSortedWith (AbsF . snd) $ map fst _G_Delta
                    -- remove diagonal matrices from ham
    ham4             = flip deleteSigmas'Ham ham3 $ map fst diag'
                    -- distribute _G1
    _G2              = catMaybes [ icomm'GT gL gR
                                 | gLRs@((gL,_):_) <- init $ tails _G1,
                                   gR <- mapHead (scale'GT 0.5) $ map snd gLRs ]
                    -- extract diagonal terms
    (diag'',_G3)    = partition isDiag $ h3'==0 ? [] $ Map.toList $ fromList'MapGT _G2 -- mergeUnionsBy (comparing fst) (\(g,c) (_,c') -> (g,c+c')) $ map pure _G2
                    -- keep only max_rg_terms terms
    (_G4, trash')   = cut_terms True max_rg_terms _G3
    
    cut_terms :: Bool -> Maybe Int -> [SigmaTerm] -> ([SigmaTerm],[SigmaTerm])
    cut_terms sortQ = maybe (,[]) (\max_terms -> splitAt max_terms . if' sortQ sort'GT id)
    
    rg' = rg {c4_ham0'RG        = c4_ham0',
              ham'RG            = ham4 +# _G4,
              diag'RG           = diag +# [diag',diag''],
              unusedIs'RG       = unusedIs',
              g4_H0Gs'RG        = g4_H0Gs' ++ g4_H0Gs,
              offdiag_errors'RG = offdiag_error : offdiag_errors,
              trash'RG          = (trash':) <$> trash,
              stab0'RG          = (g3', h3'):stab0,
              stab1'RG          = (g3 , h3 ):stab1,
              stab'RG           = stabilizers rg'}
    
--     isDiag :: SigmaTerm -> Bool
--     isDiag (g,_) = (keysSet' $ ik'G g) `isSubsetOf'` usedIs'
--       where keysSet' x = IntMap.keysSet x
--             isSubsetOf' x y = IntSet.isSubsetOf x y
    
--     isDiag :: SigmaTerm -> Bool
--     isDiag (g,_) = IntSet.null $ IntSet.intersection unusedIs' $ IntMap.keysSet $ ik'G g
    
    isDiag :: SigmaTerm -> Bool -- TODO get rid of nUnused >= size_g below
    isDiag (g,_) = not $ True {-nUnused >= size_g-} ? (any (flip IntSet.member unusedIs') $ IntMap.keys $ ik'G g)
                                           $ (any (flip IntMap.member $ ik'G g ) $ IntSet.toList unusedIs')
      --where size_g  = size'G g
      --      nUnused = IntSet.size unusedIs'
    
    find_G4s :: Sigma -> Sigma -> [Sigma]
    find_G4s g0 g1 = maybe (g0 == g1 ? [] $ {-assert (acommQ g_ g1 && acommQ g0 g_) $-} find_G4s g_ g1 ++ find_G4s g0 g_) (pure . fst) $ icomm g0 g1
      where g_        = sigma 0 $ IntMap.singleton i3 1
          --(i0s,i1s) = both (IntSet.intersection unusedIs . IntMap.keysSet . ik'G) (g0,g1)
          --i01s      = IntSet.intersection i0s i1s
          --is        = map IntSet.findMin $ IntSet.null i01s ? [i0s,i1s] $ [i01s]
          --g_'       = IntMap.fromListWith error_ $ [(i, head $ [1,2,3] \\ map (IntMap.findWithDefault 0 i . ik'G) [g0,g1]) | i <- is]

stabilizers :: RG -> Ham
stabilizers rg = c4s'Ham (-1) (g4s'RG rg) $ fromList'Ham (ls'RG rg) $ stab0'RG rg

runRG :: RG -> RG
runRG = until (IntSet.null . unusedIs'RG) rgStep

-- return: RMS of <g> over all eigenstates
-- Ham: the stabilizer Ham
rms'G :: Num a => Ham -> Sigma -> a
rms'G stab g0 = boole $ null $ acommSigmas g0 stab

anderson_corr :: Int -> [Int] -> [(Sigma,Sigma)] -> Ham -> [(Int,[(Double,Double)])]
anderson_corr  z xs ggs stab = [(x, [anderson_corr_ stab $ map snd $ anderson_corr_gs_ z x gg $ ls'Ham stab | gg <- ggs]) | x <- xs]

anderson_corr' :: Int -> [Int] -> [(Sigma,Sigma)] -> RG  -> [(Int,[(F,F)])]
anderson_corr' z xs ggs rg   = [(x, [anderson_corr'_ rg  $           anderson_corr_gs_ z x gg $ ls'RG rg | gg <- ggs]) | x <- xs]

anderson_corr_gs_ :: Int -> Int -> (Sigma,Sigma) -> [Int] -> [(Int, IntMap Int)]
anderson_corr_gs_ z x0 (g0,g0') ls | isJust gg = [(i, g_ i) | i <- [0,z..n-1]]
                                   | otherwise = []
  where n  = lY      * head ls
        lY = product $ tail ls
        gg :: Maybe Sigma -- g(0) g(x0)
        gg | x0 == 0 && g0==g0' = Just g0
           | otherwise          = (fst<$>) $ acomm g0 $ sigma 0 $ IntMap.mapKeys (flip mod n . (+ x0*lY)) $ ik'G g0'
        g_ :: Int -> IntMap Int -- g(i) g(i+x0)
        g_ i = IntMap.mapKeys (i_xs ls . zipWithExact (+) (xs_i ls i) . xs_i ls) $ ik'G $ fromJust gg

anderson_corr_ :: Ham -> [      IntMap Int ] -> (Double,Double)
anderson_corr_ _    [] = (0,0)
anderson_corr_ stab gs = meanError $ map (rms'G stab . sigma 0) gs

anderson_corr'_ :: RG -> [(Int, IntMap Int)] -> (F,F)
anderson_corr'_ _  [] = (0,0)
anderson_corr'_ rg gs = meanError $ IntMap.elems $ IntMap.fromListWith (+) $ ([(i+1,0) | (i,_) <- gs]++) $ map (\(g,c) -> (iD'G g, c*c)) $ wolf_gs
  where wolf_gs = filter (all (==3) . ik'G . fst) $ Map.toList $ gc'Ham
                $ c4s_wolff_'Ham True (max_wolff_terms'RG rg) 1 (reverse $ g4_H0Gs'RG rg)
                $ fromList'Ham (ls'RG rg) $ map (\(i,g) -> (sigma (i+1) g,1)) gs

ee_local :: IntSet -> Ham -> Double
ee_local is stab = (0.5*) $ fromIntegral $ symmeterize_rankZ2_old $ acommMat $
    [sigma 0 g'
    | g <- map ik'G $ toList $ NestedFold $ nearbySigmas' (IntSet.toList is) stab,
      let g' = intersection' g ks,
      not (IntMap.null g') && ((/=) `on` IntMap.size) g g']
  where acommMat gs = [replicate n False ++ [acommQ g g' | g' <- gs'] | (n, (g:gs')) <- zip [1..] (tails gs)]
        ks          = IntMap.fromSet (const ()) is
        intersection' x y = IntMap.intersection x y

ee1d_slow :: [Int] -> Ham -> [(Int,Double,Double)]
ee1d_slow l0s = map (\(l0,_,ee,er) -> (l0,ee,er)) . ee1d_slow' l0s [0]

ee1d_slow' :: [Int] -> [Int] -> Ham -> [(Int,Int,Double,Double)]
ee1d_slow' l0s x0s stab =
    [uncurry (l0,x0,,) $ meanError [ee_local (is l0 x `IntSet.union` is l0 (x+x0)) stab | x <- [0..lx-1]]
    | l0 <- l0s, x0 <- x0s]
  where n       = n'Ham stab
        lx      =           head $ ls'Ham stab
        lY      = product $ tail $ ls'Ham stab
        modn i  = mod i n
        is l0 x = IntSet.fromList $ map modn [x*lY..(x+l0)*lY-1]

-- mutual information (not the same as wiki Multivariate_mutual_information!)
mutual_information :: [IntSet] -> Ham -> Double
mutual_information regions stab = sum (map ee $ drop1s regions) - (genericLength regions - 1) * ee regions
  where ee is         = ee_local (IntSet.unions is) stab
        drop1s []     = []
        drop1s (x:xs) = xs : map (x:) (drop1s xs)

-- entanglement entropy: arguments: list of region sizes and stabilizer Ham
ee1d :: [Int] -> [Int] -> Ham -> [(Int,Int,Double,Double)]
ee1d l0s x0s stab = map (\(l0,x0,ees) -> uncurry (l0,x0,,) $ meanError ees) $ ee1d_ l0s x0s stab

ee1d_ :: [Int] -> [Int] -> Ham -> [(Int,Int,[Double])]
ee1d_ l0s x0s stab = [(l0, x0, [regionEE l0 x0 x | x <- [0..lx-1]]) | l0 <- l0s, x0 <- x0s]
  where
    n  = n'Ham stab
    lx =           head $ ls'Ham stab
    lY = product $ tail $ ls'Ham stab
    -- entanglement entropy of the regions [x..x+l0-1],[x+x0..x+x0+l0-1]
    regionEE :: Int -> Int -> Int -> Double
    regionEE l0 x0 x = (0.5*) $ fromIntegral $ rankZ2 $ acommMat regionStabs
      where regionStabs = zipWith sigma [0..] [g'
                          | g <- map ik'G $ Set.toList localStabs' ++ nonlocalStabs,
                            let g' = (IntMap.union `on` selRegion g . (*lY)) x (modx$x+x0),
                            not (IntMap.null g') && ((/=) `on` IntMap.keys) g g']
            localStabs' = Set.unions $ map (findWithDefault' Set.empty & flip $ cutStabs) $ nub $ map modx [x,x+l0,x+x0,x+x0+l0]
            findWithDefault' x_ y_ z_ = IntMap.findWithDefault x_ y_ z_
            -- intersect g with region [i..i+l-1]
            selRegion :: IntMap Int -> Int -> IntMap Int
            selRegion g i = (if i+l<n then                fst . IntMap.split        (i+l)
                                      else IntMap.union $ fst $ IntMap.split (modn $ i+l) g)
                          $ snd $ IntMap.split (i-1) g
            l = l0*lY
    modx x = mod x lx
    modn i = mod i n
    acommMat :: [Sigma] -> IntMap IntSet
    acommMat gs  = foldl' f IntMap.empty gs
      where  gs' = (zero'Ham $ ls'Ham stab) +# map (,1::F) gs
             add iD1 iD2 = IntMap.insertWith IntSet.union iD1 $ IntSet.singleton iD2
             f mat0 g0 = foldl'_ (\iD -> add iD0 iD . add iD iD0) iDs mat0
               where iDs = map iD'G $ filter (\g -> iD'G g < iD0 && acommQ g g0) $ map fst $ acommCandidates g0 gs'
                     iD0 = iD'G g0
    -- local stabilizers cut by [x..x+lx/2-1] where 0 <= i < lx/2 due to symmetry
    -- TODO: I think this works even if lx is odd, but I'm not certain
    cutStabs :: IntMap (Set Sigma)
    cutStabs = IntMap.unionsWith Set.union
             $ map (\g -> IntMap.fromListWith error_
                        $ map (,Set.singleton g) $ cutRegions $ intUnion $ map (flip div lY) $ IntMap.keys $ ik'G g) localStabs
      where cutRegions :: [Int] -> [Int]
            cutRegions xs = concatMap region0 $ zipExact xs (tail xs ++ [head xs])
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

weighted_rands_x :: Hashable a => a -> Int -> [(Double,Int)]
weighted_rands_x seed l = map ((\x -> (c * sqrt (1+x*x), flip mod l $ round x)) . sinh)
                         $ Random.randomRs (-lnL,lnL) $ Random.mkStdGen $ Hashable.hash ("weighted_rands_x", seed)
  where lnL = asinh $ fromIntegral l / 2
        c   = lnL / (fromIntegral l / 2)

weighted_rands_xs :: Hashable a => a -> [Int] -> [(Double,[Int])]
weighted_rands_xs seed ls = map (\wxs -> (product $ map fst wxs, map snd wxs)) $ transpose [weighted_rands_x (seed,n::Int) l | (n,l) <- zip [1..] ls]

rands_x :: Hashable a => a -> Int -> [Int]
rands_x seed l = Random.randomRs (0,l-1) $ Random.mkStdGen $ Hashable.hash ("rands_x", seed)

rands_xs :: Hashable a => a -> [Int] -> [[Int]]
rands_xs seed ls = transpose [rands_x (seed,n::Int) l | (n,l) <- zip [1..] ls]

i_xs :: [Int] -> [Int] -> Int
i_xs ls xs = foldl' (\i (l,x) -> i*l + mod x l) 0 $ zipExactNote "i_xs" ls xs

i_xs' :: [Int] -> [Int] -> Int
i_xs' ls xs = i_xs ls $ xs ++ replicate (length ls - length xs) 0

xs_i :: [Int] -> Int -> [Int]
xs_i ls i0 = map snd $ init $ scanr (\l (i,_) -> divMod i l) (i0,0) ls

-- site -> randoms -> (SigmaTerms, unused_randoms)
type ModelGen = [Int] -> [F] -> ([SigmaTerm], [F])

init_generic'Ham :: Int -> ([Int],ModelGen) -> Ham
init_generic'Ham seed (ls,model) = fromList'Ham ls $ filter ((/=0) . snd) $ concat
                                $ flip State.evalState (randoms seed) $
                                  mapM (State.state . model) $ mapM (\l -> [0..l-1]) ls

data Model = RandFerm | Ising | XYZ | XYZ2 | MajChain | Haldane | HaldaneOpen | Cluster | ClusterOpen | ToricCode | Z2Gauge | Fibonacci
  deriving (Eq, Show, Read, Enum)

-- basic_gen :: [Int] -> ([([([Int],Int)],F)],a) -> ([Int],([SigmaTerm],a))
-- basic_gen ls (gs,rs) = ([fromList'GT 0 ([(i_xs ls xs,k) | (xs,k) <- g],c) | (g,c) <- gs], rs)

type RawSigmaTerm = ([([Int],Int)],F)

basic_gen :: [Int] -> ([Int] -> [F] -> ([RawSigmaTerm], [F])) -> ([Int],ModelGen)
basic_gen ls gen = (ls, (\(gs,rs) -> ([fromList'GT 0 ([(i_xs ls xs,k) | (xs,k) <- g],c) | (g,c) <- gs], rs)) .* gen)

model_gen :: Model -> [Int] -> [F] -> ([Int],ModelGen)
model_gen RandFerm ls [p] = (ls, gen)
  where n          = product ls
        gen [0] rs = foldl' gen' ([],rs) $
                       [ [(x,3)] | x <- [0..n-1] ] ++
                       [ [(x1,k1),(x2,k2)] ++ [(x,3) | x <- [x1+1..x2-1]] |
                         x1 <- [0..n-1], x2 <- [x1+1..n-1], k1 <- [1,2], k2 <- [1,2]]
        gen [_] rs = ([], rs)
        gen  _  _  = error "RandFerm"
        gen' (terms,rp:rh:rs') g = (if' (rp<p) [fromList'GT 0 (g,rh)] [] ++ terms, rs')
        gen' _                 _ = error_
model_gen Ising ls [j,k,h] = basic_gen ls gen
  where (kj,kh) = (3,1)
        gen [x] (rj:rk:rh:rs) =
          ([ ([([x],kj),([x+1],kj)],j*rj), ([([x],kh),([x+1],kh)],k*rk), ([([x],kh)],h*rh) ], rs)
        gen [x,y] (rjx:rjy:rkx:rky:rh:rs) =
          ([ ([([x,y],kj),([x+1,y  ],kj)],j*rjx), ([([x,y],kh),([x+1,y  ],kh)],k*rkx), ([([x,y],kh)],h*rh),
             ([([x,y],kj),([x  ,y+1],kj)],j*rjy), ([([x,y],kh),([x  ,y+1],kh)],k*rky) ], rs)
        gen _ _ = error "Ising"
model_gen XYZ ls j = (ls, gen)
  where gen [x] rs_ = let (r,rs) = splitAt 3 rs_ in
          ([ set_maj'G (maj x k) & first $ fromList'GT 0 ([(i_xs ls [x0],k) | x0 <- [x,x+1]], j!!(k-1) * r!!(k-1)) | k<-[1..3]], rs)
        gen _ _ = error "XYZ"
        maj x 3 = maj x 1 ++ maj x 2
        maj x k = let b = boole (even x == (k==2)) in map (flip mod $ 2*the ls) [2*x+b,2*(x+1)+b]
model_gen XYZ2 ls js | length js == 3*3 = basic_gen ls gen -- [jx,jy,jz,jx2,jy2,jz2,jx',jy',jz']
  where gen [x] rs_ = let [j,j2,j'] = partitions 3 js
                          ([r,r2,r'],rs) = first (partitions 3) $ splitAt (3*3) rs_ in
          (concat [concat [
            [([([x  ],k+1),([x+1],k+1)], j !!k * r !!k),
             ([([x-1],k+1),([x+1],k+1)], j2!!k * r2!!k)],
            [([([x+i],mod (k+p*i) 3 +1) | i<-[-1..1]], j'!!k * r'!!k) | p<-[-1,1]] ]| k<-[0..2]], rs)
        gen _ _ = error "XYZ2"
model_gen XYZ2 ls [jx,jy,jz] = model_gen XYZ2 ls [jx,jy,jz,0.1,0,0,0,0,0]
model_gen MajChain ls [t,   g   ] = model_gen MajChain ls [t,t,g,g]
model_gen MajChain ls [t,t',g,g'] = basic_gen ls gen
  where (kt,kt') = (3,1) -- corr_z_xs_ggs depends on this
        gen [x] (rt:rt':rg:rg':rs) =
          ([  ([([x],kt )            ],  t *rt ),
              ([([x],kt'),([x+1],kt')],  t'*rt'),
              ([([x],kt ),([x+1],kt )],  g *rg ),
              ([([x],kt'),([x+2],kt')],  g'*rg')
            ], rs)
        gen _ _ = error "MajChain"
model_gen model ls [j,j'] | model == Haldane || model == HaldaneOpen = basic_gen (ls++[2]) gen
  where gen [x,0] rs_ =  let ((r,r'),rs) = first (splitAt 3) $ splitAt 6 rs_ in
          ( [([([x,1],k),([x,2],k)],j*r!!(k-1)) | k <- [1..3]] ++
            ( model == HaldaneOpen && x == head ls-1 ? []
            $ [([([x,2],k),([x+1,1],k)],j'*r'!!(k-1)) | k <- [1..3]])
           , rs)
        gen [_,1] rs = ([],rs)
        gen _     _  = error $ show model
model_gen model ls [a,b,b'] | model == Cluster || model == ClusterOpen = basic_gen ls gen
  where (ka,kb) = (3,1)
        gen [x] (   rb:rs) | model == ClusterOpen && elem x [0,head ls-1] =
          ([ ([([x],ka)],(even x ? b $ b')*rb)
           ],rs)
        gen [x] (ra:rb:rs) =
          ([ ([([x-1],kb),([x],ka),([x+1],kb)],a*ra),
             ([([x],ka)],(even x ? b $ b')*rb)
           ], rs)
        gen _ _ = error $ show model
model_gen ToricCode ls [a,b',b,a'] = basic_gen (ls++[2]) gen
  where (ka,kb) = (3,1)
        gen [x,y,0] (ra:rb:ra'1:ra'2:rb'1:rb'2:rs) =
          ([ ([([x,y,1],ka),([x+1,y,2],ka),([x,y+1,1],ka),([x,y,2],ka)],a*ra), ([([x,y,1],kb)],b'*rb'1), ([([x,y,2],kb)],b'*rb'2),
             ([([x,y,1],kb),([x-1,y,1],kb),([x,y-1,2],kb),([x,y,2],kb)],b*rb), ([([x,y,1],ka)],a'*ra'1), ([([x,y,2],ka)],a'*ra'2)], rs)
        gen [_,_,1] rs = ([],rs)
        gen _       _  = error "ToricCode"
model_gen Z2Gauge ls [a,b',b,a'] = basic_gen (ls++[3]) gen
  where (ka,kb,kA,kB) = (3,1,3,1)
        c = sum $ map abs $ [a,b',b,a']
        gen [x,y,0] (ra:rb:ra'1:ra'2:rb'1:rb'2:rs) =
          ([ ([([x,y,1],ka),([x+1,y,2],ka),([x,y+1,1],ka),([x,y,2],ka)],a*ra), ([([x,y,1],kb)],b'*rb'1), ([([x,y,2],kb)],b'*rb'2),
             ([([x,y,0],kB)],b*rb), ([([x,y,0],kA),([x,y,1],ka),([x+1,y  ,0],kA)],a'*ra'1),
                                    ([([x,y,0],kA),([x,y,2],ka),([x  ,y+1,0],kA)],a'*ra'2),
             ([([x,y,0],kB),([x,y,1],kb),([x-1,y,1],kb),([x,y-1,2],kb),([x,y,2],kb)],c)], rs)
        gen [_,_,_] rs = ([],rs)
        gen _       _  = error "Z2Gauge"
model_gen _ _ _ = error "model_gen"

model_gen_apply_gamma :: Double -> ([Int],ModelGen) -> ([Int],ModelGen)
model_gen_apply_gamma gamma (ls,gen) = (ls, first (map $ second $ gamma==0 ? (boole . (/=0)) $ (`powF` gamma)) .* gen)

-- [bins (upper bound)] -> [(bin,x)] -> [(bin,xs)]
generic_histo_ :: [Int] -> [(Int,a)] -> [(Int,[a])]
generic_histo_ xs = IntMap.toList . foldl'
                      (\hist (x,y) -> (flip $ IntMap.alter $ Just . (y:) . fromJust) hist
                                      $ fst $ fromJust $ IntMap.lookupGE x hist)
                      (IntMap.fromListWith error_ $ map (,[]) xs)

-- Meta'G & Meta'RG data

default_meta'G   :: Meta'G
merge_meta'G     :: Meta'G -> Meta'G -> Meta'G
g4s_meta'G       :: Sigma -> Sigma
init_meta'RG     :: RG -> Meta'RG
update_meta'RG   :: RG -> RG -> RG
--
has_majQ         :: Bool
set_maj'G        :: [Int] -> Sigma -> Sigma
get_maj'G        :: Sigma -> IntSet
get_majHistos'RG :: RG -> [MajHisto]

type Meta'G      = ()
default_meta'G   = ()
merge_meta'G     = const
g4s_meta'G       = id
type Meta'RG     = ()
init_meta'RG     = const ()
update_meta'RG _ = id
--
has_majQ         = False
set_maj'G _      = id
get_maj'G        = error_
get_majHistos'RG = error_

-- type Meta'G           = Maybe IntSet
-- default_meta'G        = Nothing
-- merge_meta'G          = myForce .* liftM2 xor'IntSet
-- g4s_meta'G            = set_maj'G []
-- type Meta'RG          = [MajHisto] -- [RG step -> [(# majorana, [coefficients])]]
-- init_meta'RG rg       = [calc_majHisto rg]
-- update_meta'RG rg rg' = rg' { ham'RG = {-cut_ham $-} ham'RG rg',
--                               meta'RG = calc_majHisto rg' : meta'RG rg }
--   where
--     cut_ham    ham = flip deleteSigmas'Ham ham $ filter (not . majCut) $ Map.keys $ gc'Ham $ ham
--       where majCut = (<=4) . IntSet.size . get_maj'G
-- --
-- has_majQ              = True
-- set_maj'G             = set_meta'G . Just . IntSet.fromList
-- get_maj'G             = fromJust . meta'G
-- get_majHistos'RG      = meta'RG

-- MajHisto

type MajHisto = [(Int,[F])]
-- type MajHisto = [(Int,Int)]

calc_majHisto :: RG -> MajHisto
calc_majHisto = map (\xs -> (fst $ head xs, map snd xs)) . groupWith fst . map (first $ IntSet.size . get_maj'G) . Map.toList . gc'Ham . ham'RG
-- calc_majHisto = map (\xs -> (fst $ head xs, length xs)) . groupWith fst . map (first $ IntSet.size . get_maj'G) . Map.toList . gc'Ham . ham'RG

-- float type

type_F   :: String
toF      :: Double -> F
toDouble :: F -> Double
powF     :: F -> Double -> F -- preserves sign of base
logF     :: F -> Double

-- type F   = Double
-- type_F   = "Double"
-- toF      = id
-- toDouble = id
-- powF     = \x y -> signum x * (abs x ** y)
-- logF     = log

type F   = LogFloat
type_F   = "LogFloat"
toF      = fromDouble'LF
toDouble = toDouble'LF
powF     = pow'LF
logF     = log'LF

-- main

prettyPrint :: Bool
prettyPrint = False

main :: IO ()
main = do
  let small_lsQ               = False
      max_rg_terms_default    = "32"
      max_wolff_terms_default = "4"
  
  args <- getArgs
  when (length args < 5) $ do
    putStrLn $ "usage: SBRG random-seed model [" ++ if' small_lsQ "" "ln2 " ++ "system lengths] [coupling constants] gamma"
            ++ " {max RG terms = " ++ max_rg_terms_default ++ "} {max wolff terms = " ++ max_wolff_terms_default ++ "}"
    putStr   $ "available models: "
    print    $ enumFrom $ (toEnum 0 :: Model)
    putStrLn $ "example: SBRG 0 Ising [6] [1,1,1] 1"
    exitFailure
  
  (seed,model,ln2_ls,couplings,gamma,max_rg_terms_,max_wolff_terms_) <- getArgs7 [max_rg_terms_default, max_wolff_terms_default]
    :: IO (Int, Model, [Int], [F], Double, Int, Int)
  
  let alterSeedQ     = True
      ternaryQ       = False
      calc_EEQ       = True
      calc_aCorrQ    = model /= ToricCode
      calc_aCorr'Q   = False -- length ln2_ls <= 1
      detailedQ      = False
      cut_powQ       = True
      keep_diagQ     = length ln2_ls <= 1
      full_diagQ     = False
      lrmiQ          = False
    --calc_momentsQ  = False
      calc_corrLenQ  = True
  
  let max_rg_terms    = justIf' (>=0) max_rg_terms_
      max_wolff_terms = justIf' (>=0) max_wolff_terms_
      ls0      = small_lsQ ? ln2_ls $ map (2^) ln2_ls
      ls       = ls'RG rg0
      n0       = product ls0
      n        = n'RG rg0
      l_1d     = length ls0 == 1 ? Just n $ Nothing
      z        = n // n0
      seed'    = not alterSeedQ ? seed $ Hashable.hash $ show (seed, model, ln2_ls, couplings, gamma)
      rg0      = init'RG model ls0 $ init_generic'Ham seed' $ model_gen_apply_gamma gamma $ model_gen model ls0 couplings
      rg       = runRG $ rg0 { diag'RG            = ternaryQ || not keep_diagQ ? NoDiag
                                                                               $ full_diagQ ? diag'RG rg0
                                                                                            $ Diag'MapTinyGT $ empty'MapTinyGT l_1d,
                               trash'RG           = Nothing,
                               max_rg_terms'RG    = max_rg_terms,
                               max_wolff_terms'RG = max_wolff_terms }
      xs       = ternaryQ  ? [head ls//2]
               $ (small_lsQ ? id $ filter isPow2) [1..head ls//2]
    --xs_small = [1..min (div (head ls0+1) 2) (2*head ln2_ls)]
  
  unless (0 < n) $ error_
  
  putStr   "version:            "; putStrLn "160616.0" -- year month day . minor
  putStr   "model:              "; print $ show model
  putStr   "Ls:                 "; print ls0
  putStr   "couplings:          "; print couplings
  putStr   "Γ:                  "; print gamma
  putStr   "seed:               "; print seed
  putStr   "seed':              "; print seed'
  putStr   "max RG terms:       "; print max_rg_terms
  when calc_aCorr'Q $ do
    putStr "max Anderson terms: "; print max_wolff_terms
  putStr   "float type:         "; print type_F
  
  when detailedQ $ do
    putStr "Hamiltonian: "
    print $ ham0'RG rg0
  
  let n_missing = length $ takeWhile ((==0) . snd) $ stab0'RG rg
      cut_pow2 = reverse . if' (cut_powQ && not small_lsQ) (filter $ isPow2 . fst) id . zip [1..]
      ordered_stab = map (c4s (-1) (g4s'RG rg)) $ stab0'RG rg
  
  putStr "missing stabilizers: "
  print $ n_missing
--mapM_ print $ map (toLists'GT ls) $ take 6 ordered_stab
  
  unless ternaryQ $ do
    putStr "Wolff errors: "
    print $ cut_pow2 $ wolff_errors'RG rg
    
    putStr "offdiag errors: "
    print $ cut_pow2 $ offdiag_errors'RG rg
    
    putStr "stabilizer energies: "
    print $ cut_pow2 $ map snd $ stab0'RG rg
    
    putStr "stabilizer sizes: "
    print $ cut_pow2 $ map (size'G . fst) $ ordered_stab
    
    when (length ls0 == 1) $ do
      putStr "stabilizer lengths: "
      print $ cut_pow2 $ map (length'G (head ls) . fst) $ ordered_stab
  
  when detailedQ $ do
    putStr "stabilizers: "
    print $ stab'RG rg
    
    putStr "stabilizers0: "
    print $ stab0'RG rg
    
    putStr "stabilizers1: "
    print $ stab1'RG rg
    
    putStr "i3s: "
    print $ i3s'RG rg
    
  --putStr "effective Hamiltonian: "
  --print $ diag'RG rg
    
    putStr "holographic Hamiltonian: "
    print $ c4_ham0'RG rg
  
  unless ternaryQ $ do
    let log_ns      = reverse $ takeWhile (/=0) $ iterate (flip div 2) n
        small_ns n_ = n<=n_ ? [1..n] $ [1..n_]++[n]
        
        -- [(bin upper bound, #, RSS of coefficients, sum of logs, max)]
        generic_histo :: [Int] -> (TinyG -> Int) -> [TinyGT] -> [(Int,Int,F,Double,F)]
        generic_histo ns f = map (\(n_,cs) -> (n_,length cs,rss cs, sum $ map (logF . abs) $ filter (/=0) cs, maximum $ map abs $ 0:cs)) . generic_histo_ ns . map (first f)
        
        length_histo :: [Int] -> [TinyGT] -> [(Int,Int,F,Double,F)] -- TODO d>1
        length_histo ns = generic_histo ns (fromJust . length'TinyG)
        
        all_histos :: String -> ([Int],[Int]) -> [Int] -> [TinyGT] -> IO ()
        all_histos name (nsS,nsS') nsL gcs = do
          putStr $ name ++ " size histo: "
          print $ generic_histo nsS size'TinyG gcs
          
          when (length ls0 == 1) $ do
            putStr $ name ++ " length histo: "
            print $ length_histo nsL gcs
            
            putStr $ name ++ " size-length histo: "
            print $ map (second $ length_histo nsL) $ generic_histo_ nsS' $ map (\gT@(g,_) -> (size'TinyG g, gT)) gcs
        
        toTinys = map (first $ to'TinyG l_1d) . Map.toList
    
    putStr "small stabilizers: " 
    print $ map (first $ size'G)
          $ uncurry (++) $ second (take 20) $ span ((==0) . snd) $ sortWith (abs . snd) $ toList'Ham $ stab'RG rg
    
    all_histos "stabilizer"      (log_ns     , log_ns     ) log_ns         $ toTinys $ gc'Ham $ stab'RG rg
    all_histos "diag"            (small_ns 32, small_ns 16) log_ns & mapM_ $
      case diag'RG rg of
           NoDiag           -> Nothing
           Diag'MapGT     m -> Just $ toTinys m
           Diag'MapTinyGT m -> Just $ Map.toList $ gc'MapTinyGT m
--  all_histos "c4 ham0"         (log_ns     , log_ns     ) log_ns         $ Map.toList  $  gc'Ham  $   c4_ham0'RG rg 
--  all_histos "diag c4 ham0"    (log_ns     , log_ns     ) log_ns         $ filter (all (==3) . ik'G . fst) $ Map.toList  $ gc'Ham  $   c4_ham0'RG rg 
--  all_histos "offdiag c4 ham0" (log_ns     , log_ns     ) log_ns         $ filter (not . all (==3) . ik'G . fst) $ Map.toList  $ gc'Ham  $   c4_ham0'RG rg 
  
  when calc_EEQ $ do
    let mapMeanError = map (\(l0,x0,ees) -> uncurry (l0,x0,,) $ meanError ees)
        entanglement_data = ee1d_ xs (small_lsQ || ternaryQ ? [0] $ 0:xs) $ stab'RG rg
        entanglement_map = Map.fromListWith error_ $ map (\(l,x,es) -> ((l,x),es)) entanglement_data
        lrmi_data = mapMeanError $ flip mapMaybe entanglement_data $
                      \(l,x,es) -> let es0 = entanglement_map Map.! (l,0) in
                                       justIf (x>=l) (l, x, zipWith3 (\e0 e0' e -> e0 + e0' - e) es0 (rotateLeft x es0) es)
    
    putStr "entanglement entropy: " -- [(region size, region separation, entanglement entropy, error)]
    print $ mapMeanError entanglement_data
    
--  putStr "entanglement entropies: " -- [(region size, region separation, [entanglement entropies])]
--  print $ ee1d_ [1..last xs] [0] $ stab'RG rg
    
    putStr "long range mutual information: "
    print $ lrmi_data
    
    when lrmiQ $ do
      putStrLn "LRMI: "
      mapM_ print [(l0, mapMaybe (\(l,x,e,de) -> justIf (l==l0 && x>=head ls//4) (x,e,de)) lrmi_data) | l0 <- init $ init $ xs]
  
--   when calc_momentsQ $ do
--     let rand_iss samples n_ = take samples $ groups $ map (*z) $ Random.randomRs (0,n0-1) $ Random.mkStdGen $ Hashable.hash (seed', "rand_iss", samples, n_)
--           where groups = (\(is0,is') -> is0 : groups is') . splitAt n_
--         calc_moments :: Int -> [Int] -> ([Int] -> Double) -> [(Int,Double,Double)]
--         calc_moments samples ns_ f = [ uncurry (n_,,) $ meanError $ map f $ rand_iss samples n_ | n_ <- ns_ ]
--         
--         mutual_information_moment is = case map IntSet.singleton $ IntSet.toList $ foldl1 xor'IntSet $ map IntSet.singleton is of
--                                             []      -> 1
--                                             regions -> mutual_information regions $ stab'RG rg
--         anderson_moment = uncurry (*) . bimap (rms'G $ stab'RG rg) toDouble . foldl1 (fromJust .* acomm'GT) . map ((,1) . sigma 0 . flip IntMap.singleton 3) -- TODO ToricCode
--     
--     putStr "mutual information moments: " -- [(n, avg MI(i1,..,in), error)]
--     print $ calc_moments (4*n) [2,3,4,6] mutual_information_moment
--     
--     putStr "anderson moments: " -- [(n, avg <Q_i1 .. Q_in>^2, error)]
--     print $ calc_moments (32*n) [2,4,8] anderson_moment
  
  when calc_corrLenQ $ do
    let l0 = head ls0
        mqs = [(0,0),(2,0),(4,0),(6,0),(0,1),(0,2)]
        
        structure_factor :: (Functor v, Floating (v Double), MySeq (v Double))
                         => (Int -> Int -> v Double) -> [(Int,Int,v Double,v Double)]
        structure_factor f = zipWithExact (\(m,q) (s,ds) -> (m,q,s,ds)) mqs
                           $ map (both (fromIntegral n0*^) . weightedMeanError' (*^)) $ transpose
                           $ [ {-(f0!!1 >=0 ? id $ traceShow (w,mod (head dxs + head ls0//2) (head ls0) - (head ls0)//2,f0)) $-}
                               [ (w, (sin_^m * cos_) *^ f0)
                               | (m,q) <- mqs,
                                 let k0  = 2*pi/fromIntegral l0
                                     cos_ = q==0 ? 1 $ mean $ map (\x -> cos $ k0 * fromIntegral (q*x)) dxs' ]
                             | let seed_ = (seed',"structure_factor"),
                               (xs_,(w,dxs)) <- --[([x],(1,[y])) | x <- [0..head ls-1], y <- [0..head ls-1]],
                                                take (round $ (\n_ -> n_*log n_) (2*fromIntegral n0 :: Double))
                                              $ zip (         rands_xs seed_ ls0)
                                                    (weighted_rands_xs seed_ ls0),
                               let ys_   = zipWithExact (+) xs_ dxs
                                   f0    = (f `on` i_xs' ls) xs_ ys_
                                   dxs'  = catMaybes $ zipWithExact justIf (map (==l0) ls0) dxs
                                   sin_  = product $ zipWithExact ((\l x -> l/pi * sin (pi*x/l)) `on` fromIntegral) ls0 dxs ]
        
        offset f = mean [f i j | i <- [0,z .. n],
                                 let j = i_xs' ls $ zipWith (+) (map (//2) ls0) $ xs_i ls i ]
        
        ising_f k = \i j -> let f0 = f i j
                            in  {-assert (i /= j || f0 /= 0)-} V2 (f0, f0 - offset_)
          where f       = curry $ uncurry (*) . bimap (rms'G $ stab'RG rg) toDouble . fromJust . uncurry acomm . both (sigma 0 . flip IntMap.singleton k)
                offset_ = offset f
        
        anderson_fs = case model of
                           Ising -> [("ZZ", ising_f 3)]
                           XYZ   -> [("XX", ising_f 1), ("YY", ising_f 2), ("ZZ", ising_f 3)]
                           _     -> []
        
        mutual_information_f = \i j -> let f0 = f i j
                                       in {-assert (f0 >= head (ising_f 3 i j)) $-} V3 (f0, f0 - offset_, fc i j)
          where ee is     = ee_local (IntSet.fromList is) (stab'RG rg)
                f i j     = cMI i j []
                offset_   = offset f
                cMI i j k = i == j ? 1 $ (\i' j' -> ee (i'++k) + ee (j'++k) - ee (i'++j'++k) - ee k) (unit_cell i) (unit_cell j) -- TODO make faster since k can be large
                fc  i j   = cMI i j $ map (i_xs' ls) $ sequenceA $ (zipWith3 g ls0 `on` xs_i ls) i j
                  where g l x y = (\k -> [k-l//8+1..k+l//8-boole (even $ x+y)]) $ snd $ head $ reverse $ sortWith fst
                                $ map (\x' -> (reverse $ sort $ map (modDist l . (x'-)) [x,y], x'))
                                $ (\x' -> [x',x'+l//2]) $ quot (x + y) 2
                unit_cell i = [i..i+z-1]
    
    forM_ anderson_fs $ \(name,f) -> do
      putStr $ "anderson " ++ name ++ " structure factor: " -- [(m, q, [ZZ(q)^m, ZZ - offset], error)]
      print $ structure_factor f
    
    putStr "mutual information structure factor: " -- [(m, q, [MI(q)^m, MI - offset, conditional MI], error)]
    print $ structure_factor mutual_information_f
  
  let corr_z_xs_ggs = third3 (map $ both $ sigma 0 . IntMap.fromListWith error_) $ case model of
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
  
  when (has_majQ && model == XYZ) $ do
    putStr "majorana histo: "
    print $ cut_pow2 $ tail $ get_majHistos'RG rg
  
--   print $ length $ show rg
  
  putStr "CPU time: "
  cpu_time <- getCPUTime
  print $ (1e-12::Double) * fromIntegral cpu_time


-- old entanglement code

--     let a = IntSet.fromList [0..1]
--         c = IntSet.fromList [4..5]
--         b = IntSet.fromList [head ls - i | i <- [1..2]]
--         s as = ee_local (IntSet.unions as) (stab'RG rg)
--     putStr "I(A:B): "
--     print $ s[a] + s[b] - s[a,b]
--     putStr "I(A:B:C): "
--     print $ s [a] + s [b] + s [c] - s [a,b] - s [b,c] - s [c,a] + s[a,b,c]
--     putStr "I(A:B|C): "
--     print $ s [a,c] + s [b,c] - s [a,b,c] - s[c]
    
--     let a = IntSet.fromList [    2,3]
-- --         c = IntSet.fromList [0,1,    4,5,   8,9,      12,13]
--         c = IntSet.fromList [        4,5,             12,13]
-- --         c = IntSet.fromList [        4,5,   8,9          ]
--         b = IntSet.fromList [                   10,11]
--         s as = ee_local (IntSet.unions as) (stab'RG rg)
--     putStr "I(A:B): "
--     print $ s[a] + s[b] - s[a,b]
--     putStr "I(A:B:C): "
--     print $ s [a] + s [b] + s [c] - s [a,b] - s [b,c] - s [c,a] + s[a,b,c]
--     putStr "I(A:B|C): "
--     print $ s [a,c] + s [b,c] - s [a,b,c] - s[c]
    
--     print $ [(lx,ly,-2*(fromIntegral$lx+ly) + ee_local (IntSet.fromList [i_xs ls [x,y,a] | x <-[1..lx], y<-[1..ly], a<-[0,1]]) (stab'RG rg)) |
--               lx <- [2,4,8], ly <- [2,4,8] ]
--     do
--       let a = IntSet.fromList [i_xs ls [x,y,q] | x<-[1..3], y<-[1..4], q<-[0,1]]
--           b = IntSet.fromList [i_xs ls [x,y,q] | x<-[4..7], y<-[1..2], q<-[0,1]]
--           c = IntSet.fromList [i_xs ls [x,y,q] | x<-[4..7], y<-[3..4], q<-[0,1]]
--           s as = ee_local (IntSet.unions as) (stab'RG rg)
--       print $ (s [a], s [a,b], s [a,b,c])
--       print $ s [a] + s [b] + s [c] - s [a,b] - s [b,c] - s [c,a] + s[a,b,c]
--       let a  = IntSet.fromList [i_xs ls [x,y,q] | x<-[1..2], y<-[1..6], q<-[0,1]]
--           b  = IntSet.fromList [i_xs ls [x,y,q] | x<-[5..6], y<-[1..6], q<-[0,1]]
--           c  = IntSet.fromList [i_xs ls [x,y,q] | x<-[3..4], y<-[1,2,5,6], q<-[0,1]]
--           s as = ee_local (IntSet.unions as) (stab'RG rg)
--       print $ s [a,c] + s [b,c] - s [a,b,c] - s[c]

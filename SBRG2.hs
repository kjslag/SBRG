-- author: Kevin Slagle
-- github.com/kjslag/SBRG

-- cabal --enable-profiling v2-install --lib  clock hashable ieee754 NumInstances random safe  bitwise
-- optional: parallel

{-# LANGUAGE CPP, TupleSections, BangPatterns, MagicHash, MultiParamTypeClasses, FlexibleInstances #-} -- OverloadedLists
-- :set -XTupleSections

{-# OPTIONS_GHC -Wall -Wno-unused-top-binds -Wno-unused-imports -O #-} --  -fllvm -fexcess-precision -optc-ffast-math -optc-O3
-- -rtsopts -prof -fprof-auto        -ddump-simpl -threaded
-- +RTS -xc -s -p                    -N4
-- +RTS -hy && hp2ps -c SBRG2p.hp && okular SBRG2p.ps

-- ghci -fbreak-on-error
-- :set -fbreak-on-error
-- :set args 1 XYZ [4] [1,1,1] 1
-- :trace main

-- default:
#ifndef FERMIONIC
#ifndef BOSONIC
#define FERMIONIC
#endif
#endif

-- #define PARALLEL

import GHC.Prim (reallyUnsafePtrEquality#)
import GHC.Exts (sortWith, the)

-- import Control.Exception.Base (assert)
import Control.Monad
import Control.Monad.ST
import Data.Bifunctor
import Data.Bits
import Data.Containers.ListUtils
import Data.Either
import Data.Function
import Data.Foldable
import Data.Hashable
import Data.Int
import Data.Ix
import Data.List
import Data.Maybe
import Data.NumInstances.Tuple
import Data.Ord
import Data.Word
import Debug.Trace
import Numeric.IEEE (nan, infinity, succIEEE, predIEEE)
import Safe
import Safe.Exact
import System.Environment
import System.Exit (exitFailure)

import qualified System.CPUTime as CPUTime
import qualified System.Clock as Clock
--import qualified GHC.Stats as GHC

#ifdef PARALLEL
import qualified Control.Parallel as Parallel
import qualified Control.Parallel.Strategies as Parallel
#endif

import qualified System.Random as Random

--import Control.Monad.Trans.State.Strict (State)
import qualified Control.Monad.Trans.State.Strict as State

import Data.IntSet.Internal (IntSet(..))
import qualified Data.IntSet.Internal as IntSet
import qualified Utils.Containers.Internal.BitUtil as BitUtil

import Data.Set (Set)
import qualified Data.Set as Set

import Data.IntMap.Strict (IntMap)
import qualified Data.IntMap.Strict as IntMap hiding (fromList, insert, delete, adjust, adjustWithKey, update, updateWithKey) -- use alter instead

import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map hiding (fromList, insert, delete, adjust, adjustWithKey, update, updateWithKey) -- use alter instead

import Data.Array (Array)
import Data.Array.Unboxed (UArray)
import Data.Array.IArray (IArray)
import qualified Data.Array.IArray as Array

-- import Data.Array.BitArray (BitArray)
-- import qualified Data.Array.BitArray as BitArray

import Data.Array.BitArray.ST (STBitArray)
import qualified Data.Array.BitArray.ST as STBitArray

-- import Data.Vector.Unboxed (Vector)
-- import qualified Data.Vector.Unboxed as Vector

-- debug

-- error_ :: a
-- error_ = error "compile with '-rtsopts -prof -fprof-auto' and run with '+RTS -xc' for a stack trace"

todo :: a
todo = undefined

assert :: String -> Bool -> a -> a
assert _ True  = id
assert s False = error s

asserting :: String -> (a -> Bool) -> a -> a
asserting s q x = assert s (q x) x

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

(***) :: (a -> a') -> (b -> b') -> (a, b) -> (a', b')
(f *** g) (x, y) = (f x, g y)

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

-- nest' :: Int -> (a -> a) -> a -> a
-- nest' 0 _ !x = x
-- nest' n f !x = f $ nest (n-1) f x

newtype NestedFold t1 t2 a = NestedFold { getNestedFold :: t1 (t2 a) }

instance (Foldable t1, Foldable t2) => Foldable (NestedFold t1 t2) where
  foldr  f x = foldr  (flip $ foldr  f) x . getNestedFold
  foldl' f x = foldl' (       foldl' f) x . getNestedFold

getArgs7 :: (Read t1, Read t2, Read t3, Read t4, Read t5, Read t6, Read t7) => [String] -> IO (t1,t2,t3,t4,t5,t6,t7)
getArgs7 defaults = do
  args <- getArgs
  let n = 7
      [x1,x2,x3,x4,x5,x6,x7] = args ++ drop (length defaults + length args - n) defaults
  return (readNote "1" x1, readNote "2" x2, readNote "3" x3, readNote "4" x4, readNote "5" x5, readNote "6" x6, readNote "7" x7)

infixl 1 `applyIf`
applyIf :: (a -> a) -> (a -> Bool) -> a -> a
applyIf f q x = if' (q x) f id $ x

justIf :: Bool -> a -> Maybe a
justIf True  = Just
justIf False = const Nothing

justIf' :: (a -> Bool) -> a -> Maybe a
justIf' f x = justIf (f x) x

foldl'IntSet :: (IntSet.Prefix -> IntSet.BitMap -> a -> a) -> IntSet -> a -> a
foldl'IntSet f = go
  where go (Bin _ _ l r) !x = go r $ go l x
        go (Tip p b) !x = f p b x
        go Nil x = x

{-# SCC xor'IntSet #-}
xor'IntSet :: IntSet -> IntSet -> IntSet
xor'IntSet x y = IntSet.union x y `IntSet.difference` IntSet.intersection x y

array'Array :: (IArray a e, Ix i) => (i,i) -> (i -> e) -> a i e
array'Array bounds f = Array.listArray bounds $ f <$> range bounds

-- array'BitArray :: Ix i => (i,i) -> (i -> e) -> a i e
-- array'BitArray bounds f = BitArray.listArray bounds $ f <$> range bounds

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

#ifdef PARALLEL
myParListChunk :: MySeq a => Int -> [a] -> [a]
myParListChunk n | parallelQ = Parallel.withStrategy $ Parallel.parListChunk n $ Parallel.rseq . myForce
                 | otherwise = id

par, pseq :: a -> b -> b
par  = parallelQ ? Parallel.par  $ flip const
pseq = parallelQ ? Parallel.pseq $ flip const
#endif

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

-- sparse Z2Matrix

data Z2Mat = Z2Mat (IntMap IntSet) (IntMap IntSet)

fromSymmetric'Z2Mat :: IntMap IntSet -> Z2Mat
fromSymmetric'Z2Mat mat = Z2Mat mat mat

{-# SCC popRow'Z2Mat #-}
popRow'Z2Mat :: Z2Mat -> Maybe (IntSet, Z2Mat)
popRow'Z2Mat (Z2Mat rows cols) = case IntMap.minViewWithKey rows of
    Nothing               -> Nothing
    Just ((i,row), rows') -> let cols' = IntSet.foldl' (flip $ IntMap.alter $ justIf' (not . IntSet.null) . IntSet.delete i . fromJust) cols row
                             in Just (row, Z2Mat rows' cols')

{-# SCC xorRow'Z2Mat #-}
xorRow'Z2Mat :: IntSet -> Int -> Z2Mat -> Z2Mat
xorRow'Z2Mat row i (Z2Mat rows cols) = Z2Mat rows' cols'
  where rows'     = IntMap.alter (xorMay row . fromJust) i rows
        cols'     = IntSet.foldl' (flip $ IntMap.alter $ maybe (Just i0) $ xorMay i0) cols row
        i0        = IntSet.singleton i
        xorMay    = justIf' (not . IntSet.null) .* xor'IntSet

-- sparse rankZ2

{-# SCC sparse_rankZ2 #-}
sparse_rankZ2 :: IntMap IntSet -> Int
sparse_rankZ2 = go 0 . fromSymmetric'Z2Mat
-- rankZ2 m0 = traceShow m0 $ traceShow (IntMap.size m0, fst $ IntMap.findMax m0, meanError $ map (fromIntegral . IntSet.size) $ IntMap.elems m0) $ go 0 $ fromSymmetric'Z2Mat m0
-- rankZ2 m0 = (IntMap.size m0 > 50 ?? let x=fst $ IntMap.findMax m0 in traceShow [[boole $ IntSet.member j (IntMap.findWithDefault IntSet.empty i m0) | j <- [-x .. x]] | i <- [-x .. x]]) $ go 0 $ fromSymmetric'Z2Mat m0
  where
    go :: Int -> Z2Mat -> Int
    go !n mat_ = case popRow'Z2Mat mat_ of
        Nothing             -> n
        Just (row, mat@(Z2Mat _ cols)) -> go (n+1) $
            let j  = IntSet.findMin row
            in case IntMap.lookup j cols of
                    Nothing -> mat
                    Just is -> IntSet.foldl' (flip $ xorRow'Z2Mat row) mat is

-- dense rankZ2

-- {-# SCC dense_rankZ2'Integer #-}
-- dense_rankZ2'Integer :: [Integer] -> Int
-- dense_rankZ2'Integer = go 0
--   where go :: Int -> [Integer] -> Int
--         go  n []         = n
--         go  n (0:rows)   = go n rows
--         go !n (row:rows) = go (n+1) $ map (xor row `applyIf` flip testBit j) rows
--           where j = head $ filter (testBit row) [0..]

-- using zipWithTo
-- {-# SCC dense_rankZ2 #-}
-- dense_rankZ2 :: [STBitArray s Int] -> ST s Int
-- dense_rankZ2 = go 0
--   where go :: Int -> [STBitArray s Int] -> ST s Int
--         go  n []          = return n
--         go !n (row0:rows) = maybe (go n rows) f =<< STBitArray.elemIndex True row0
--           where f j = traverse_ g rows >> go (n+1) rows
--                   where g row = do q <- STBitArray.readArray row j
--                                    q ? STBitArray.zipWithTo row xor row0 row $ return ()

{-# SCC dense_rankZ2 #-}
dense_rankZ2 :: [STBitArray s Int] -> ST s Int
dense_rankZ2 = go 0
  where go :: Int -> [STBitArray s Int] -> ST s Int
        go  n []          = return n
        go !n (row0:rows) = maybe (go n rows) f =<< STBitArray.elemIndex True row0
          where f j = go (n+1) =<< traverse g rows
                  where g row = do q <- STBitArray.readArray row j
                                   q ? STBitArray.zipWith xor row0 row $ return row

-- this ends up being slower, probably due to the strange ordering due to lazyness
-- {-# SCC dense_rankZ2 #-}
-- dense_rankZ2 :: [BitArray Int] -> Int
-- dense_rankZ2 = go 0
--   where go :: Int -> [BitArray Int] -> Int
--         go  n []          = n
--         go !n (row0:rows) = maybe (go n rows) `flip` Array.elemIndex True row0 $ \j ->
--                             go (n+1) $ (xor row0 `applyIf` (Array.! j)) <$> rows

-- boolsToInteger :: [Bool] -> Integer
-- boolsToInteger = foldl' (\x b -> 2*x + boole b) 0
-- 
-- rankZ2_old :: [[Bool]] -> Int
-- rankZ2_old = dense_rankZ2 . map boolsToInteger

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

-- if bosonic, is'G (IntSet.singleton $ 2^(2*i)*k) -> [1,x,y,z] operator at i if k=[0,1,2,3]
-- if fermionic, is'G should always have even size, and if size is 2 mod 4, then an implicit factor of i is included
--               and indices should be positive integers (due to multSigma)
data Sigma = Sigma {
  is'G   :: !IntSet, -- Index set
  hash'G ::  SigmaHash }

sigma :: IntSet -> Sigma
sigma ik = Sigma ik $ calc_hash'G ik

check'G :: Sigma -> Bool
check'G g = bosonicQ || even (IntSet.size $ is'G g) && (IntSet.findMin $ is'G g) >= 0 -- multSigma probably assumes is'G are non-negative

{-# SCC calc_hash'G #-}
calc_hash'G :: IntSet -> SigmaHash
calc_hash'G g = assert "calc_hash'G" (finiteBitSize (0::Int) == 64) $ foldl'IntSet h g r0
  where h p b s = r2*(rotateL (r1*s) 31 `xor` p) `xor` fromIntegral b
        [r0,r1,r2] = [-999131058408291745, 5388482987636913043, -4581131298018549302] -- random Ints
-- calc_hash'G = hash . IntSet.toList

test_hash'G :: Bool
test_hash'G = False

-- used to test hash quality
assert_unique'G :: IntSet -> IntSet -> a -> a
assert_unique'G g1 g2 = assert ("assert_unique'G:\n" ++ intercalate "\n" (show . IntSet.toList <$> [g1,g2])) $ ptrEquality g1 g2 || g1 == g2

pos_i'G :: Index -> Pos
pos_i'G | fermionicQ = id
        | otherwise  = (`div` 2)

xs_i'G :: Ls -> Index -> Xs
xs_i'G ls = xs_pos ls . pos_i'G

positions'G :: Sigma -> IntSet
positions'G | fermionicQ = is'G
            | otherwise  = IntSet.map pos_i'G . is'G

instance Hashable Sigma where
  hashWithSalt salt = hashWithSalt salt . hash'G
  hash = hash'G

instance MySeq Sigma

instance Eq Sigma where
  Sigma g1 hash1 == Sigma g2 hash2 = test_hash'G && hashEq ? assert_unique'G g1 g2 hashEq $ hashEq
    where hashEq = hash1 == hash2

instance Ord Sigma where
  Sigma g1 hash1 `compare` Sigma g2 hash2 = not test_hash'G ? hashOrd $
      case hashOrd of EQ -> assert_unique'G g1 g2 EQ; o -> o
    where hashOrd = compare hash1 hash2

instance Show Sigma where
  showsPrec n = showsPrec n . toList'G

-- instance Read Sigma where
--   readsPrec n = map (first fromList'G) . readsPrec n

type SigmaTerm = (Sigma, F)

toList'G :: Sigma -> [Index]
toList'G = IntSet.toList . is'G

toList'GT :: SigmaTerm -> ([Index],F)
toList'GT = first toList'G

fromList'G :: [Index] -> Sigma
fromList'G is = asserting "fromList'G check'G" check'G . sigma . asserting "fromList'G unique" ((== length is) . IntSet.size) $ IntSet.fromList is

fromListB'G :: [(Pos,Int)] -> Sigma
fromListB'G = assert "fromListB'G" bosonicQ $ fromList'G . concat . map f
  where f (i,k) = case k of 0 -> []
                            1 -> [2*i]
                            2 -> [2*i+1]
                            3 -> [2*i, 2*i+1]
                            _ -> error "fromListB'G"

fromList'GT :: ([Index],F) -> SigmaTerm
fromList'GT (is,c) = (fromList'G is, fermionicQ && odd (sF is) ? -c $ c)
  where sF :: [Int] -> Int
        sF (x0:xs0) = go ([],0) ([],0) xs0 0
           where go :: ([Int],Int) -> ([Int],Int) -> [Int] -> Int -> Int
                 go (ls,nl) (rs,nr) (x:xs) !s | x < x0    = go (x:ls, nl+1) (  rs, nr  ) xs $ s + nl + 1 + nr
                                              | x > x0    = go (  ls, nl  ) (x:rs, nr+1) xs $ s + nr
                                              | otherwise = error "fromList'GT"
                 go (ls, _) (rs, _) [] s = s + sF ls + sF rs
        sF [] = 0

fromListB'GT :: ([(Int,Int)],F) -> SigmaTerm
fromListB'GT = first fromListB'G

show'GTs :: [SigmaTerm] -> String
show'GTs = if' prettyPrint (concat . intersperse " + " . map (\(g,c) -> show c ++ " σ" ++ show g)) show

sort'GT :: [(a,F)] -> [(a,F)]
sort'GT = sortWith (AbsF . snd)

scale'GT :: F -> SigmaTerm -> SigmaTerm
scale'GT x (g,c) = (g,x*c)

{-# SCC length'G #-} 
length'G :: Int -> Sigma -> Int
length'G l g | null xs   = 0
             | otherwise = l - maximum (modDifferences l xs)
  where xs = IntSet.toList $ positions'G g

{-# SCC size'G #-} 
size'G :: Sigma -> Int
size'G = IntSet.size . positions'G


acommQ_slow :: Sigma -> Sigma -> Bool
acommQ_slow g1 g2 = odd $ IntSet.size $ is'G g1 `IntSet.intersection` f (is'G g2)
  where f | fermionicQ = id
          | otherwise  = IntSet.map $ \i -> even i ? i+1 $ i-1

{-# SCC acommQ #-}
acommQ :: Sigma -> Sigma -> Bool
acommQ g1 g2 = intersection'IntSet (\l r -> xor $ odd $ popCount $ l .&. flip_ r) (is'G g1) (is'G g2) False
  where flip_ b = fermionicQ ? b $ unsafeShiftL (b .&. 0x5555555555555555) 1 .|. -- 5 = 0101
                                   unsafeShiftR (b .&. 0xAAAAAAAAAAAAAAAA) 1     -- A = 1010

{-# INLINE intersection'IntSet #-}
-- modified from https://hackage.haskell.org/package/containers-0.6.1.1/docs/src/Data.IntSet.Internal.html#intersection
intersection'IntSet :: (IntSet.BitMap -> IntSet.BitMap -> a -> a) -> IntSet -> IntSet -> a -> a
intersection'IntSet f = intersection
  where intersection t1@(Bin p1 m1 l1 r1) t2@(Bin p2 m2 l2 r2) !x
          | shorter m1 m2  = intersection1
          | shorter m2 m1  = intersection2
          | p1 == p2       = intersection r1 r2 $ intersection l1 l2 x
          | otherwise      = x
          where intersection1 | nomatch p2 p1 m1  = x
                              | zero p2 m1        = intersection l1 t2 x
                              | otherwise         = intersection r1 t2 x
                intersection2 | nomatch p1 p2 m2  = x
                              | zero p1 m2        = intersection t1 l2 x
                              | otherwise         = intersection t1 r2 x
        intersection t1@(Bin _ _ _ _) (Tip kx2 bm2) !x = intersectBM t1
          where intersectBM (Bin p1 m1 l1 r1) | nomatch kx2 p1 m1 = x
                                              | zero kx2 m1       = intersectBM l1
                                              | otherwise         = intersectBM r1
                intersectBM (Tip kx1 bm1) | kx1 == kx2 = f bm1 bm2 x
                                          | otherwise  = x
                intersectBM Nil = x
        intersection (Bin _ _ _ _) Nil !x = x
        intersection (Tip kx1 bm1) t2 !x = intersectBM t2
          where intersectBM (Bin p2 m2 l2 r2) | nomatch kx1 p2 m2 = x
                                              | zero kx1 m2       = intersectBM l2
                                              | otherwise         = intersectBM r2
                intersectBM (Tip kx2 bm2) | kx1 == kx2 = f bm1 bm2 x
                                          | otherwise  = x
                intersectBM Nil = x
        intersection Nil _ !x = x
        zero = IntSet.zero
        nomatch i p m = (mask i m) /= p
        shorter m1 m2 = (natFromInt m1) > (natFromInt m2)
        mask i m = maskW (natFromInt i) (natFromInt m)
        maskW i m = intFromNat (i .&. (complement (m-1) `xor` m))
        natFromInt :: Int -> Word
        natFromInt i = fromIntegral i
        intFromNat :: Word -> Int
        intFromNat w = fromIntegral w

-- f and g are applied from left to right in the index order, but the integers are ordered like 0,1,2,-2,-1
{-# INLINE union'IntSet #-}
-- modified from https://hackage.haskell.org/package/containers-0.6.1.1/docs/src/Data.IntSet.Internal.html#union
union'IntSet :: (Either IntSet IntSet -> a -> a) -> (IntSet.BitMap -> IntSet.BitMap -> a -> a) -> IntSet -> IntSet -> a -> a
union'IntSet g f = union_
  where union_ t1@(Bin p1 m1 l1 r1) t2@(Bin p2 m2 l2 r2) !x
          | shorter m1 m2  = union1
          | shorter m2 m1  = union2
          | p1 == p2       = union_ r1 r2 $ union_ l1 l2 x
          | otherwise      = link p1 t1 p2 t2 x
          where
            union1  | nomatch p2 p1 m1  = link p1 t1 p2 t2 x
                    | zero p2 m1        = g (Left r1) $ union_ l1 t2 x
                    | otherwise         = union_ r1 t2 $ g (Left l1) x

            union2  | nomatch p1 p2 m2  = link p1 t1 p2 t2 x
                    | zero p1 m2        = g (Right r2) $ union_ t1 l2 x
                    | otherwise         = union_ t1 r2 $ g (Right l2) x
        union_ t@(Bin _ _ _ _) (Tip kx bm) !x = insertBM kx bm False t x
        union_ t@(Bin _ _ _ _) Nil !x = g (Left t) x
        union_ (Tip kx bm) t !x = insertBM kx bm True t x
        union_ Nil t !x = g (Right t) x
        insertBM !kx !bm rQ t@(Bin p m l r) !x
          | nomatch kx p m = rQ ? link kx (Tip kx bm) p t x
                                $ link p t kx (Tip kx bm) x
          | zero kx m      = g (lr r) $ insertBM kx bm rQ l x
          | otherwise      = insertBM kx bm rQ r $ g (lr l) x
          where lr = rQ ? Right $ Left
        insertBM kx bm rQ t@(Tip kx' bm') !x
          | kx' == kx = rQ ? f bm  bm' x
                           $ f bm' bm  x
          | otherwise = rQ ? link kx (Tip kx bm) kx' t x
                           $ link kx' t kx (Tip kx bm) x
        insertBM kx bm rQ Nil !x = flip g x $ (rQ ? Left $ Right) $ Tip kx bm
        link p1 t1 p2 t2 !x
          | zero p1 m = g (Right t2) $ g (Left  t1) x
          | otherwise = g (Left  t1) $ g (Right t2) x
          where m = branchMask p1 p2
        zero = IntSet.zero
        nomatch i p m = (mask i m) /= p
        shorter m1 m2 = (natFromInt m1) > (natFromInt m2)
        mask i m = maskW (natFromInt i) (natFromInt m)
        maskW i m = intFromNat (i .&. (complement (m-1) `xor` m))
        branchMask p1 p2 = intFromNat (BitUtil.highestBitMask (natFromInt p1 `xor` natFromInt p2))
        natFromInt :: Int -> Word
        natFromInt i = fromIntegral i
        intFromNat :: Word -> Int
        intFromNat w = fromIntegral w

{-# SCC multSigma #-} 
multSigma :: Sigma -> Sigma -> (Int,Sigma)
multSigma (Sigma g1_ _) (Sigma g2_ _) | bosonicQ  = (sB `mod` 4, g_)
                                      | otherwise = ((sF + sf g1_ + sf g2_ - sf (is'G g_)) `mod` 4, g_)
  where g_ = {-# SCC "g_" #-} sigma $ xor'IntSet g1_ g2_
        
        sB = {-# SCC "sB" #-} intersection'IntSet f g1_ g2_ 0
          where f b1 b2 = (+) $ sum [fromIntegral $ multSigma_datB Array.! (h b1, h b2) | i <- [0,n .. 64-n], let h b = unsafeShiftR b i .&. mask]
                mask = 0xFF -- F = 1111
                n = 8
        
        --sB_slow :: Int
        --sB_slow = {-# SCC "sB_slow" #-} sum $ map (\i -> even i /= i `IntSet.member` union_ ? 1 $ -1) $ IntSet.toList $ IntSet.intersection g1_ $ IntSet.map flip_ g2_
        -- where union_  = IntSet.union g2_ $ IntSet.map flip_ g1_
        --       flip_ i = even i ? i+1 $ i-1
        
        sf g = case IntSet.size g `mod` 4 of
                    0 -> 0
                    2 -> 1
                    _ -> error "multSigma"
        
        (0, sF) = {-# SCC "sF" #-} second (2*) $ union'IntSet g f g1_ g2_ (IntSet.size g1_, 0)
          where g (Left  is) (!n1,!s) = (n1 - IntSet.size is, s)
                g (Right is) (!n1,!s) = (n1, s + n1 * IntSet.size is)
                f b1 b2 (!n1,!s) = (n1 - popCount b1,) $ (s+) $ popCount 
                                 $ (b2 .&.) $ (even n1 ? id $ complement) $ go 1 b1
                  where go 64 !b = b
                        go i  !b = go (2*i) $ b `xor` unsafeShiftL b i
        
        --sF_slow = {-# SCC "sF_slow" #-} 2 * f (IntSet.size g1_) (IntSet.toAscList g1_) (IntSet.toAscList g2_) 0
        --  where f :: Int -> [Int] -> [Int] -> Int -> Int
        --        f n1 (i1:g1) (i2:g2) !s | i1 < i2   = f (n1-1)     g1  (i2:g2)   s
        --                                | i1 > i2   = f  n1    (i1:g1)     g2  $ s + boole (odd  n1)
        --                                | otherwise = f (n1-1)     g1      g2  $ s + boole (even n1)
        --        f _ _ _ s = s

{-# SCC multSigma_datB #-} 
multSigma_datB :: UArray (Word,Word) Int8
multSigma_datB = array'Array ((0,0), (2^n-1,2^n-1)) $ \(b1,b2) -> sum [dat0 Array.! (h b1, h b2) | i <- [0,2 .. n-2], let h b = unsafeShiftR b i .&. 3]
  where dat0 :: UArray (Word,Word) Int8
        dat0 = Array.listArray ((0,0),(3,3)) [0, 0, 0, 0,
                                              0, 0, 1,-1,
                                              0,-1, 0, 1,
                                              0, 1,-1, 0]
        n = 8

-- i [a,b]/2
icomm :: Sigma -> Sigma -> Maybe SigmaTerm
icomm g1 g2 = case s of
                   0 -> Nothing
                   2 -> Nothing
                   1 -> Just (g,-1)
                   3 -> Just (g, 1)
                   _ -> undefined
  where (s,g) = multSigma g1 g2

-- {a,b}/2
acomm :: Sigma -> Sigma -> Maybe SigmaTerm
acomm g1 g2 = case s of
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

{-# SCC localQ'Sigma #-} 
localQ'Sigma :: Int -> Sigma -> Bool
localQ'Sigma n = \g -> size'G g < round max_n
  where max_n = sqrt $ fromIntegral n :: Double
--where max_n = log (fromIntegral n) / log 2 + 16 :: Double

-- MapGT

type MapGT = Map Sigma F

{-# SCC fromList'MapGT #-}
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

{-# SCC toSortedList'MASs #-}
toSortedList'MASs :: [MapAbsSigmas] -> [SigmaTerm]
toSortedList'MASs gs_ = [ (g',c) | (AbsF c,gs) <- mergeUnionsBy (comparing fst) (\(c,s) (_,s') -> (c, Set.union s s')) $ map Map.toAscList gs_,
                                   g' <- Set.toList gs ]

-- Ham

-- MapAbsSigmas is used so that acommCandidates_sorted is ordered by coefficient so that max_wolff_terms cuts is more efficient
-- TODO get rid of that?
data Ham = Ham {
  gc'Ham    :: !MapGT,                 -- Hamiltonian:  sigma matrix  -> coefficient
  lcgs'Ham  :: !MapAbsSigmas,          -- |coefficient| ->     local sigma matrices
  nlcgs'Ham :: !MapAbsSigmas,          -- |coefficient| -> non-local sigma matrices
  icgs'Ham  :: !(IntMap MapAbsSigmas), -- local sigmas: site index (/2 if bosonicQ) -> |coefficient| -> local sigma matrices
  ls'Ham    :: !Ls }                   -- system lengths

instance Show Ham where
  show = show . Map.toList . gc'Ham

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

null'Ham :: Ham -> Bool
null'Ham = Map.null . gc'Ham

{-# SCC insert'Ham #-}
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
  where c_     = Map.lookup g gc
        localQ = localQ'Sigma (n'Ham ham) g
        {-# SCC lcgs' #-}
        lcgs'  = lcgs +# gT
        {-# SCC nlcgs' #-}
        nlcgs' = nlcgs +# gT
        {-# SCC icgs' #-}
        icgs'  = let cgs_  = Map.singleton (AbsF c) $ Set.singleton g
                 in          IntMap.mergeWithKey (\_ gc0 _ -> Just $ gc0 +# gT) id id icgs $ IntMap.fromSet (const cgs_) $ positions'G g

delete'Ham :: Sigma -> Ham -> Ham
delete'Ham = fst .* deleteLookup'Ham

{-# SCC deleteLookup'Ham #-}
deleteLookup'Ham :: Sigma -> Ham -> (Ham, F)
deleteLookup'Ham g ham@(Ham gc lcgs nlcgs icgs ls) =
  (Ham gc'
       (localQ ?  lcgs' $  lcgs )
       (localQ ? nlcgs  $ nlcgs')
       (localQ ?  icgs' $  icgs )
       ls, c)
  where localQ        = localQ'Sigma (n'Ham ham) g
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

{-# SCC acommCandidates #-}
acommCandidates :: Sigma -> Ham -> [SigmaTerm]
acommCandidates = (\(nlgs:lgs) -> toList'MAS nlgs ++ toSortedList'MASs lgs) .* acommCandidates_
  -- toSortedList'MASs is needed so that we take unions of g

{-# SCC acommCandidates_sorted #-}
acommCandidates_sorted :: Sigma -> Ham -> [SigmaTerm]
acommCandidates_sorted = toSortedList'MASs .* acommCandidates_

{-# SCC acommCandidates_ #-}
acommCandidates_ :: Sigma -> Ham -> [MapAbsSigmas]
acommCandidates_ g ham = nlcgs'Ham ham : IntMap.elems localSigmas
  where localSigmas = IntMap.restrictKeys (icgs'Ham ham) $ positions'G g

nearbySigmas :: Pos -> Ham -> MapAbsSigmas
nearbySigmas i ham = nearbySigmas' [i] ham

{-# SCC nearbySigmas' #-}
nearbySigmas' :: [Pos] -> Ham -> MapAbsSigmas
nearbySigmas' is ham = nlcgs'Ham ham +# [IntMap.lookup i $ icgs'Ham ham | i <- is]

-- the Maybe SigmaTerm has a +/- 1 coefficient
icomm_sorted'Ham :: SigmaTerm -> Ham -> [(Maybe SigmaTerm, F)]
icomm_sorted'Ham (g,c) ham = map (\(g',c') -> (icomm g g', c*c')) $ acommCandidates_sorted g ham

{-# SCC c4'Ham #-}
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

-- RG

type G4_H0G = Either Sigma (F,Int,Int) -- Either g4 (rms of icomm H_0 Sigma/h_3^2, i3, j3)
-- if bosonic, the i3 bit and not the j3 bit is filled for the new stabilizer

data RG = RG {
  ls0'RG             :: ![Int],
  ham'RG             :: !Ham,
  ham_sizes'RG       :: ![Int],
  diag'RG            :: !(Maybe MapGT),
  unusedIs'RG        :: !IntSet,
  g4_H0Gs'RG         :: ![G4_H0G],
  parity_stab'RG     :: !Bool,             -- RG found the parity stab
  offdiag_errors'RG  :: ![F],
  trash'RG           :: !(Maybe [[SigmaTerm]]),
  stab0'RG           :: ![SigmaTerm],      -- stabilizers in new basis
  stab1'RG           :: ![SigmaTerm],      -- stabilizers in current basis
  stab'RG            ::  Ham,              -- stabilizers in old basis
  max_rg_terms'RG    :: !(Maybe Int),
  n_rg_terms'RG      :: ![Int],
  bifurcation'RG     :: !Bifurcation,
  randGen'RG         :: !Random.StdGen}
  deriving (Show)

instance MySeq RG where
  mySeq rg = id -- deepseq rg
           . seq' (head . ham_sizes'RG)
           . seq' diag'RG
           . seq' (head . g4_H0Gs'RG)
           . seq' (head . offdiag_errors'RG)
           . seq' ((head<$>) . trash'RG)
           . seq' (head . stab0'RG)
           . seq' (head . stab1'RG)
           . seq' (head . n_rg_terms'RG)
    where seq' f = mySeq $ f rg

ls'RG :: RG -> [Int]
ls'RG = ls'Ham . ham'RG

n'RG :: RG -> Int
n'RG = n'Ham . ham'RG

g4s'RG :: RG -> [Sigma]
g4s'RG = lefts . g4_H0Gs'RG

wolff_errors'RG :: RG -> [F]
wolff_errors'RG = map fst3 . rights . g4_H0Gs'RG

init'RG :: [Int] -> Int -> Ham -> RG
init'RG ls0 seed ham = rg
  where rg = RG ls0 ham [Map.size $ gc'Ham ham] (Just Map.empty) (IntSet.fromDistinctAscList [0..((bosonicQ ? 2 $ 1) * n'Ham ham - 1)])
             [] False [] (Just []) [] [] (stabilizers rg) Nothing [] SB (Random.mkStdGen $ hashWithSalt seed "init'RG")

-- g ~ sigma matrix, _G ~ Sigma, i ~ site index, h ~ energy coefficient
{-# SCC rgStep #-}
rgStep :: RG -> RG
rgStep rg@(RG _ ham1 ham_sizes diag unusedIs g4_H0Gs parity_stab offdiag_errors trash stab0 stab1 _ max_rg_terms n_rg_terms bifurcation randGen)
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
    {-# SCC g4s #-} -- if not bifurcationQ, then g3' must only involve ij3
    (ij3,g3',g4s) | fermionicQ = case (i3_1, i3_2) of
                                      ([i3,j3], _) -> (Just (i3,j3), g3, [])
                                      (i3:_, j3:_) -> (Just (i3,j3), simp $ fromList'G [i3,j3], g4s_)
                                      (_, [])      -> (Nothing     , g3, []) -- is fermion parity
                                      _            -> error "rgStep ij3"
                  | otherwise  = let j3 = head i3_1
                                     i3 = odd j3 ? j3-1 $ j3+1
                                 in  (Just (i3, j3), simp $ fromList'G [i3], g4s_)
      where g4s_ = [fst $ fromJust $ icomm g3 g3']
            simp = id -- not bifurcationQ ? id $ sigma . IntSet.union (is'G g3 `IntSet.difference` unusedIs) . is'G
    unusedIs'    = maybe unusedIs `flip` ij3 $ \(i3,j3) -> unusedIs `IntSet.difference` IntSet.fromList [i3,j3]
    parity_stab' = (fermionicQ&&) $ assert "parity_stab'" (not $ isNothing ij3 && parity_stab) $ isNothing ij3 || parity_stab
    g4_H0Gs'     = (maybe [] `flip` ij3) (\(i3,j3) -> [Right (rss $ map (snd . fst) _G1, i3, j3)]) ++ map Left (reverse g4s)
    bifurcationQ = bifurcation == SB
    
    {-# SCC isDiag #-}
    isDiag :: SigmaTerm -> Bool
    isDiag (g,_) = unusedIs' `IntSet.disjoint` is'G g
                || fermionicQ && parity_stab' && unusedIs' `IntSet.isSubsetOf` is'G g
    
    {-# SCC proj #-}
    proj :: [SigmaTerm] -> [SigmaTerm]
    proj = bifurcationQ ? id $ map projGS_
      where projGS_ gT@(g,c) = maybe gT (, (h3' <= 0) == groundStateQ ? c $ -c) $ case ij3 of
                                 Just (i3,_) -> justIf (i3 `IntSet.member` is'G g) $ sigma $ is'G g IntSet.\\ is'G g3'
                                 Nothing     -> justIf (is'G g == unusedIs) $ fromList'G []
    
    (groundStateQ, randGen') = case bifurcation of
                                    SB   -> (undefined, randGen)
                                    GS -> (True, randGen)
                                    RS -> Random.random randGen
    
    {-# SCC ham2 #-} -- apply c4 transformation
    ham2           = c4s'Ham 1 g4s ham1
    {-# SCC h3' #-} -- get g3' coefficient
    h3'            = fromMaybe 0 $ Map.lookup g3' $ gc'Ham ham2
                  -- find sigma matrices that overlap with g3'
                  -- split into commuting matrices (_comm) and anticommuting commutators (_G1)
    (_comm,_G1)    = {-# SCC "diag',_G1" #-} maybe (toList'Ham ham2, []) `flip` ij3
                   $ \(i3,j3) -> partitionEithers $ map (\gT -> maybe (Left gT) (Right . (,scale'GT (-1) gT)) $ icomm'GT (g3',recip h3') gT)
                               $ toList'MAS $ nearbySigmas' (nub $ map pos_i'G [i3,j3]) ham2 -- the minus is because we apply icomm twice
    (diag',offdiag)= partition isDiag _comm
    {-# SCC ham3 #-} -- remove anticommuting matrices from ham
    ham3           = flip deleteSigmas'Ham ham2 $ map (fst . snd) _G1
    {-# SCC _G_Delta #-} -- [Sigma, Delta]
    _G_Delta       = [(icomm_sorted'Ham (g,1) ham3', c) | (g,c) <- map fst _G1]
                     where ham3' = delete'Ham g3' ham3
    {-# SCC offdiag_error #-} -- calc offdiag error
    offdiag_error  = maybe 0 (/abs h3') $ headMay $ map (abs . snd) $ filter (isJust . fst)
                   $ mergeSortedWith (AbsF . snd) $ map fst _G_Delta
    {-# SCC ham4 #-} -- remove diagonal matrices from ham and project to ground state if not bifurcationQ
    ham4           = (bifurcationQ ? id $ (+# proj offdiag))
                   $ flip deleteSigmas'Ham ham3 $ map fst $ bifurcationQ ? diag' $ _comm
    {-# SCC _G2 #-} -- distribute _G1 = [(icomm g3' gT/h3' = i[g3',gT]/2h3', -gT)] where gT is in Sigma
                    -- _G2 = g3' G^2/2h3' where G = Sigma
    _G2            = proj $ catMaybes -- $ myParListChunk 64
                     [ icomm'GT gL gR
                     | gLRs@((gL,_):_) <- init $ tails _G1,
                       gR <- mapHead (scale'GT 0.5) $ map snd gLRs ]
                  -- extract diagonal terms
    (diag'',_G3)  = {-# SCC "diag'',_G3" #-} partition isDiag $ h3'==0 ? [] $ (fastSumQ ? id $ Map.toList . fromList'MapGT) _G2
                  -- keep only max_rg_terms terms
    (_G4, trash') = {-# SCC "_G4,trash'" #-} maybe (_G3,[]) (\max_terms -> splitAt max_terms $ sort'GT $ _G3) max_rg_terms
    
    {-# SCC rg' #-}
    rg' = rg {ham'RG            = ham4 +# _G4,
              ham_sizes'RG      = (Map.size $ gc'Ham $ ham'RG rg') : ham_sizes,
              diag'RG           = (+# [proj diag',diag'']) <$> diag,
              unusedIs'RG       = unusedIs',
              g4_H0Gs'RG        = g4_H0Gs' ++ g4_H0Gs,
              parity_stab'RG    = parity_stab',
              offdiag_errors'RG = offdiag_error : offdiag_errors,
              trash'RG          = (trash':) <$> trash,
              stab0'RG          = (g3', h3'):stab0,
              stab1'RG          = (g3 , h3 ):stab1,
              stab'RG           = stabilizers rg',
              randGen'RG        = randGen',
              n_rg_terms'RG     = length _G3 : n_rg_terms}

stabilizers :: RG -> Ham
stabilizers rg = c4s'Ham (-1) (g4s'RG rg) $ fromList'Ham (ls'RG rg) $ stab0'RG rg

runRG :: RG -> RG
runRG = until (IntSet.null . unusedIs'RG) rgStep

-- return: RMS of <g> over all eigenstates
-- Ham: the stabilizer Ham
rms'G :: Num a => Ham -> Sigma -> a
rms'G stab g0 = boole $ null $ acommSigmas g0 stab

-- for debugging
-- entanglement :: Ls -> [Sigma] -> [(L,X)] -> Double
-- entanglement ls stabs = regionEE_1d ls $ calcCutStabs $ fromList'Ham ls $ map (,1) stabs

-- entanglement entropy: arguments: list of region sizes and stabilizer Ham
ee0d :: Ham -> [X] -> [(X,Double,Double)]
ee0d stab x0s = map (\(x0,ees) -> uncurry (x0,,) $ meanError ees) $ ee0d_ stab x0s

ee0d_ :: Ham -> [X] -> [(X,[Double])]
ee0d_ stab x0s = [(x0, [ee_local stab [x*lY+y, (x+x0)*lY+y]
                       | x <- [0..head ls-1],
                         y <- [0..lY-1]])
                 | x0 <- x0s]
  where ls = ls'Ham stab
        lY = product $ tail ls

-- entanglement entropy: arguments: list of region sizes and stabilizer Ham
ee1d :: Ls -> ([X] -> [Sigma]) -> [L] -> [X] -> [(L,X,Double,Double)]
ee1d ls cutStabs l0s x0s = map (\(l0,x0,ees) -> uncurry (l0,x0,,) $ meanError ees) $ ee1d_ ls cutStabs l0s x0s

ee1d_ :: Ls -> ([X] -> [Sigma]) -> [L] -> [X] -> [(L,X,[Double])]
ee1d_ ls cutStabs l0s x0s = [(l0, x0, [regionEE_1d ls cutStabs $ nub [(l0,x),(l0,x+x0)]
                                      | x <- [0..head ls-1]])
                            | l0 <- l0s,
                              x0 <- x0s]

ee_local :: Ham -> [X] -> Double
ee_local stab is = maskedStabEntanglement
    [sigma g'
    | g <- map is'G $ toList $ NestedFold $ nearbySigmas' is stab,
      let g' = IntSet.intersection g is',
      not (IntSet.null g') && g /= g']
  where is' = IntSet.fromList $ not bosonicQ ? is $ concatMap (\i -> [2*i,2*i+1]) is

-- entanglement entropy of the regions (l,x) -> [x..x+l-1]
{-# SCC regionEE_1d #-}
regionEE_1d :: Ls -> ([X] -> [Sigma]) -> [(L,X)] -> Double
regionEE_1d ls cutStabs lxs_ = maskedStabEntanglement regionStabs
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

{-# SCC maskedStabEntanglement #-}
maskedStabEntanglement :: [Sigma] -> Double
maskedStabEntanglement stabs0 = ((bosonicQ ? 0.5 $ 0.25)*) $ fromIntegral
                              $ bosonicQ ? sparse_rankZ2 acommMatB
                                         $ runST $ dense_rankZ2 =<< acommMatF
  where stabs = nubOrd stabs0
        {-# SCC acommMatB #-}
        acommMatB :: IntMap IntSet
        acommMatB = foldl'_ f [(g1,g2) | g1:gs' <- tails $ zip [1..] stabs, g2 <- gs'] IntMap.empty
          where add i j = IntMap.insertWith IntSet.union i $ IntSet.singleton j
                f ((i,gi), (j,gj)) = acommQ gi gj ? add i j . add j i $ id
        {-# SCC acommMatF #-}
        acommMatF :: ST s [STBitArray s Int]
        acommMatF = do let n = length stabs
                           bounds = (-n, n-1)
                       mat <- (Array.listArray bounds <$>) $ sequence $ replicate (2*n) $ STBitArray.newArray bounds False :: ST s (Array Int (STBitArray s Int))
                       let add    i j = STBitArray.writeArray (mat Array.! i) j True
                           addF q i j = q ? (add i j >> add j i >> add (-i-1) (-j-1) >> add (-j-1) (-i-1)) $ return ()
                       sequence_ [ addF bothOdd i (-j-1) >> addF (i /= j && (bothOdd `xor` acomm0)) i j
                                 | gs'@((i,gi,ni):_) <- tails $ zip3 [0..] stabs $ map size'G stabs,
                                   (j,gj,nj) <- gs',
                                   let acomm0  = acommQ gi gj -- true if odd # of overlapping majorana
                                       bothOdd = odd ni && odd nj ]
                       return $ Array.elems mat

-- stabilizers that may have been cut by [x..x+lx/2-1] for any x in xs
{-# SCC calcCutStabs #-}
calcCutStabs :: Ham -> ([X] -> [Sigma])
calcCutStabs stab = \xs -> localStabs' xs ++ nonlocalStabs
  where
    ls     = ls'Ham stab
    lx     = head ls
    lx_2   = lx // 2
    modx   = (`mod` lx)
    modx_2 = (`mod` lx_2)
    
    localStabs' xs = Set.toList $ Set.unions $ map (IntMap.findWithDefault Set.empty `flip` cutStabs0) $ nub $ map modx_2 xs
    localStabs, nonlocalStabs :: [Sigma]
    [localStabs, nonlocalStabs] = [toList $ NestedFold $ cgs stab | cgs <- [lcgs'Ham, nlcgs'Ham]]
    -- local stabilizers cut by [x..x+lx/2-1] where 0 <= x < lx/2 due to symmetry
    cutStabs0 :: IntMap (Set Sigma)
    cutStabs0 = IntMap.fromListWith Set.union $ concatMap f localStabs
    f :: Sigma -> [(Int,Set Sigma)]
    f g = map (,Set.singleton g) $ cutRegions $ nubInt $ map x_i $ toList'G g
      where x_i = head . xs_i'G ls
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
  Ising | XYZ | MajSquare | MajSquareOpen
#else
  MajChainF | MajSquareF
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

model_gen :: Model -> Ls -> [F] -> (Ls,ModelGen)
#ifdef BOSONIC
model_gen Ising ls [j,k,h] = basic_genB ls gen
  where gen [x] (rj:rk:rh:rs) =
          ([ ([([x],3),([x+1],3)],j*rj), ([([x],1),([x+1],1)],k*rk), ([([x],1)],h*rh) ], rs)
        gen [x,y] (rjx:rjy:rkx:rky:rh:rs) =
          ([ ([([x,y],3),([x+1,y  ],3)],j*rjx), ([([x,y],1),([x+1,y  ],1)],k*rkx), ([([x,y],1)],h*rh),
             ([([x,y],3),([x  ,y+1],3)],j*rjy), ([([x,y],1),([x  ,y+1],1)],k*rky) ], rs)
        gen _ _ = error "Ising"
model_gen XYZ ls j | length j == 3 = basic_genB ls gen
  where gen [x] rs_ = let (r,rs) = splitAt 3 rs_ in
          ([ ([([x0],k) | x0 <- [x,x+1]], j!!(k-1) * r!!(k-1)) | k<-[1..3] ], rs)
        gen _ _ = error "XYZ"
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
#else
model_gen MajChainF ls [j1,j2,k] = basic_genF ls gen
  where gen [x] (rj:rk:rs) = ([ ([[x],[x+1]], (even x ? j1 $ j2)*rj), ([[x],[x+1],[x+2],[x+3]], k*rk) ], rs)
        gen _ _ = error "MajChainF"
model_gen MajSquareF ls [ly_]     = model_gen MajSquareF ls [ly_,1,0]
model_gen MajSquareF ls [ly_,q,t] = basic_genF (ls++[ly]) gen
  where ly = round $ toDouble ly_
        gen [x,y] (rq:rt1:rt2:rs) = ([ ([[x,y],[x,y+1],[x+1,y],[x+1,y+1]], q*rq), ([[x,y],[x+1,y]], t*rt1), ([[x,y],[x,y+1]], t*rt2) ], t==0 ? rt1:rt2:rs $ rs)
        gen _ _ = error "MajSquareF"
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

data Bifurcation = SB | GS | RS
  deriving (Eq, Show, Read)

prettyPrint :: Bool
prettyPrint = False

fastSumQ :: Bool
fastSumQ = False -- this can actually cause a slowdown if lots of terms have to be added to Ham as a result

parallelQ :: Bool
parallelQ = False

{-# SCC main #-}
main :: IO ()
main = do
  startTime <- Clock.getTime Clock.Monotonic
  
  let small_lsQ               = False
      max_rg_terms_default    = "32"
    --max_wolff_terms_default = "4"
  
  args <- getArgs
  when (length args < 5) $ do
    putStrLn $ "usage: SBRG random-seed model bifurcation [" ++ if' small_lsQ "" "ln2 " ++ "system lengths] [coupling constants] gamma"
            ++ " {max RG terms = " ++ max_rg_terms_default ++ "}" -- {max wolff terms = " ++ max_wolff_terms_default ++ "}"
    putStr   $ "available models: "
    print    $ enumFrom $ (toEnum 0 :: Model)
    putStrLn $ "example: SBRG 0 " ++ (bosonicQ ? "Ising SB [6] [1,1,1]" $ "MajChainF SB [6] [1,1,1]")
    exitFailure
  
  (seed,model,bifurcation,ln2_ls,couplings,gamma,max_rg_terms_) <- getArgs7 ["1", max_rg_terms_default]
    :: IO (Int, Model, Bifurcation, [Int], [F], Double, Int)
  
--   (seed,model,ln2_ls,couplings,gamma,max_rg_terms_,max_wolff_terms_) <- getArgs7 [max_rg_terms_default, max_wolff_terms_default]
--     :: IO (Int, Model, [Int], [F], Double, Int, Int)
  
  let alterSeedQ     = True
      allStabsQ      = detailedQ
      calc_EEQ       = True
    --calc_aCorrQ    = False
    --calc_aCorr'Q   = False
      detailedQ      = False
      cut_powQ       = not detailedQ
      bifurcationQ   = bifurcation == SB
      keep_diagQ     = not bifurcationQ || detailedQ
      entanglement_wo_missing = not bosonicQ && show model == "MajSquareF" && (length couplings < 3 || couplings!!2 == 0)
  
  let max_rg_terms    = justIf' (>=0) max_rg_terms_
    --max_wolff_terms = justIf' (>=0) max_wolff_terms_
      ls0      = small_lsQ ? ln2_ls $ map (2^) ln2_ls
      dim      = length ls0
      ls       = ls'RG rg0
      n        = n'RG rg0
      l_1d     = dim == 1 ? Just n $ Nothing
      seed'    = not alterSeedQ ? hash seed $ hash $ show (seed, model, ln2_ls, couplings, gamma)
      rg0      = init'RG ls0 seed' $ init_generic'Ham seed' $ model_gen_apply_gamma gamma $ model_gen model ls0 couplings
      rg       = runRG $ rg0 { diag'RG            = keep_diagQ ? diag'RG rg0 $ Nothing,
                               trash'RG           = Nothing,
                               bifurcation'RG     = bifurcation,
                               max_rg_terms'RG    = max_rg_terms }
      xs_pow2  = filter isPow2 [1..head ls//2]
      xs_      = small_lsQ   ? [1..head ls//2] $ xs_pow2
  
  unless (0 < n) $ undefined
  
  putStr   "version:            "; putStrLn "190812.0" -- major . year month day . minor
  putStr   "warnings:           "; print $ catMaybes [justIf fastSumQ "fastSum", justIf entanglement_wo_missing "entanglement w/o missing"]
  putStr   "model:              "; print $ show model
  putStr   "Ls:                 "; print ls0
  putStr   "couplings:          "; print $ (read $ args!!4 :: [Double]) -- print couplings
  putStr   "Γ:                  "; print gamma
  putStr   "seed:               "; print seed
  putStr   "seed':              "; print seed'
  putStr   "max RG terms:       "; print max_rg_terms
--when calc_aCorr'Q $ do
--  putStr "max Anderson terms: "; print max_wolff_terms
  putStr   "float type:         "; print type_F
  putStr   "bifurcation:        "; print $ show bifurcation
  
  when detailedQ $ do
    putStr "Hamiltonian: "
    print $ toDescList'Ham $ ham'RG rg0
  
  let n_missing = length $ takeWhile ((==0) . snd) $ stab0'RG rg
      cut_pow2 = reverse . if' (cut_powQ && not small_lsQ) (filter $ isPow2 . fst) id . zip [1..]
      ordered_stab = map (c4s (-1) (g4s'RG rg)) $ stab0'RG rg
  
  putStr "missing stabilizers: "
  print $ n_missing
--mapM_ print $ map (toLists'GT ls) $ take 6 ordered_stab
  
  putStr "RG CPU time:  "
  rg_cpu_time <- CPUTime.getCPUTime
  print $ (1e-12::Double) * fromInteger rg_cpu_time
  
  putStr "RG WALL time: "
  rg_endTime <- Clock.getTime Clock.Monotonic
  print $ (1e-9::Double) * (fromInteger $ on (-) Clock.toNanoSecs rg_endTime startTime)
  
  putStr "Wolff errors: "
  print $ cut_pow2 $ wolff_errors'RG rg
  
  putStr "offdiag errors: "
  print $ cut_pow2 $ offdiag_errors'RG rg
  
  putStr "Hamiltonian sizes: "
  print $ cut_pow2 $ tail $ ham_sizes'RG rg
  
  putStr "max Hamiltonian size: "
  print $ maximum $ ham_sizes'RG rg
  
  putStr "new RG terms: "
  print $ cut_pow2 $ n_rg_terms'RG rg
  
  putStr "max new RG terms: "
  print $ maximum $ n_rg_terms'RG rg
  
  putStr "stabilizer energies: "
  print $ cut_pow2 $ map (abs . snd) $ stab0'RG rg
  
  putStr "stabilizer sizes: "
  print $ cut_pow2 $ map (size'G . fst) $ ordered_stab
  
  when (not bifurcationQ) $ do
    let ([],energy) = toList'GT $ the $ Map.toList $ fromJust $ diag'RG rg
    putStr "energy: "
    print $ energy
  
  when detailedQ $ do
    putStr "stabilizers: "
    print $ toDescList'Ham $ stab'RG rg
    
    putStr "stabilizers0: "
    print $ stab0'RG rg
    
    putStr "stabilizers1: "
    print $ stab1'RG rg
    
    case diag'RG rg of
      Just diag -> do putStr "effective Hamiltonian: "
                      print $ sortOn (abs . snd) $ Map.toList $ diag
      _         -> return ()
  
  putStr "small stabilizers: "
  print $ (allStabsQ ? id $ uncurry (++) . second (take 20) . span ((==0) . snd))
        $ sortWith snd $ map (size'G *** abs) $ toList'Ham $ stab'RG rg
  
  do
    let log_ns      = reverse $ takeWhile (/=0) $ iterate (flip div 2) n
        small_ns n_ = n<=n_ ? [1..n] $ [1..n_]++[n]
        
        -- [(bin upper bound, #, RSS of coefficients, sum of logs, max)]
        generic_histo :: [Int] -> (Sigma -> Int) -> [SigmaTerm] -> [(Int,Int,F,Double,F)]
        generic_histo ns f = map (\(n_,cs) -> (n_,length cs,rss cs, sum $ map (logF . abs) $ filter (/=0) cs, maximum $ map abs $ 0:cs)) . generic_histo_ ns . map (first f)
        
        length_histo :: [Int] -> [SigmaTerm] -> [(Int,Int,F,Double,F)]
        length_histo ns = generic_histo ns (length'G $ fromJust l_1d)
        
        all_histos :: String -> ([Int],[Int]) -> [Int] -> [SigmaTerm] -> IO ()
        all_histos name (nsS,_{-nsS'-}) nsL gcs = do
          putStr $ name ++ " size histo: "
          print $ generic_histo nsS size'G gcs
          
          when (dim == 1) $ do
            putStr $ name ++ " length histo: "
            print $ length_histo nsL gcs
            
            --putStr $ name ++ " size-length histo: "
            --print $ map (second $ length_histo nsL) $ generic_histo_ nsS' $ map (\gT@(g,_) -> (size'G g, gT)) gcs
    
    all_histos "stabilizer"      (log_ns     , log_ns     ) log_ns         $ Map.toList $ gc'Ham $ stab'RG rg
    all_histos "diag"            (small_ns 32, small_ns 16) log_ns & mapM_ $ Map.toList <$> diag'RG rg
  
  when calc_EEQ $ do
    let mapMeanError = map (\(l0,x0,ees) -> uncurry (l0,x0,,) $ meanError ees)
        entanglement_data_ cutStabs = ee1d_ ls cutStabs xs_     (small_lsQ ? [0] $ 0:xs_)
                      ++ (small_lsQ ? ee1d_ ls cutStabs xs_pow2 xs_ $ [])
        entanglement_data = entanglement_data_ $ calcCutStabs
                          $ not entanglement_wo_missing ? stab'RG rg
                          $ deleteSigmas'Ham `flip` (stab'RG rg)
                          $ map fst $ filter ((==0) . snd) $ toList'Ham $ stab'RG rg
    
    when (length ls > 1) $ do
      putStr "entanglement entropy 0d: " -- [(region separation, entanglement entropy, error)]
      print $ ee0d (stab'RG rg) $ 0:xs_
    
    putStr "entanglement entropy: " -- [(region size, region separation, entanglement entropy, error)]
    print $ mapMeanError entanglement_data
    
    let entanglement_map = Map.fromListWith undefined $ map (\(l,x,es) -> ((l,x),es)) entanglement_data
        lrmi_data = mapMeanError $ flip mapMaybe entanglement_data $
                      \(l,x,es) -> let es0 = entanglement_map Map.! (l,0) in
                                       justIf (x>=l) (l, x, zipWith3 (\e0 e0' e -> e0 + e0' - e) es0 (rotateLeft x es0) es)
    
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

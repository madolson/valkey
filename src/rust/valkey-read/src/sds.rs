//! The `static inline` sds header accessors from src/sds.h. bindgen cannot
//! bind inline functions and a C call per sdslen() would cost more than the
//! lookup, so they are mirrored here. Layouts: src/sds.h `sdshdr5`..`sdshdr64`.

use crate::sys::sds;

const SDS_TYPE_5: u8 = 0;
const SDS_TYPE_8: u8 = 1;
const SDS_TYPE_16: u8 = 2;
const SDS_TYPE_32: u8 = 3;
const SDS_TYPE_64: u8 = 4;
const SDS_TYPE_MASK: u8 = 7;
const SDS_TYPE_BITS: u8 = 3;

#[inline(always)]
unsafe fn flags(s: sds) -> u8 {
    *(s as *const u8).offset(-1)
}

/// Reads the `len` and `alloc` fields of a packed sdshdr8..64.
#[inline(always)]
unsafe fn hdr<T: Copy + Into<u64>>(s: sds) -> (usize, usize) {
    // struct { T len; T alloc; unsigned char flags; char buf[]; } packed.
    let base = (s as *const u8).sub(2 * core::mem::size_of::<T>() + 1);
    let len = core::ptr::read_unaligned(base as *const T);
    let alloc = core::ptr::read_unaligned(base.add(core::mem::size_of::<T>()) as *const T);
    (len.into() as usize, alloc.into() as usize)
}

/// sdslen() src/sds.h
#[inline(always)]
pub unsafe fn sdslen(s: sds) -> usize {
    let f = flags(s);
    match f & SDS_TYPE_MASK {
        SDS_TYPE_5 => (f >> SDS_TYPE_BITS) as usize,
        SDS_TYPE_8 => hdr::<u8>(s).0,
        SDS_TYPE_16 => hdr::<u16>(s).0,
        SDS_TYPE_32 => hdr::<u32>(s).0,
        SDS_TYPE_64 => hdr::<u64>(s).0,
        _ => 0,
    }
}

/// sdsalloc() src/sds.h
#[inline(always)]
pub unsafe fn sdsalloc(s: sds) -> usize {
    let f = flags(s);
    match f & SDS_TYPE_MASK {
        SDS_TYPE_5 => (f >> SDS_TYPE_BITS) as usize,
        SDS_TYPE_8 => hdr::<u8>(s).1,
        SDS_TYPE_16 => hdr::<u16>(s).1,
        SDS_TYPE_32 => hdr::<u32>(s).1,
        SDS_TYPE_64 => hdr::<u64>(s).1,
        _ => 0,
    }
}

/// sdsavail() src/sds.h
#[inline(always)]
pub unsafe fn sdsavail(s: sds) -> usize {
    let f = flags(s);
    match f & SDS_TYPE_MASK {
        SDS_TYPE_5 => 0,
        SDS_TYPE_8 => {
            let (l, a) = hdr::<u8>(s);
            a - l
        }
        SDS_TYPE_16 => {
            let (l, a) = hdr::<u16>(s);
            a - l
        }
        SDS_TYPE_32 => {
            let (l, a) = hdr::<u32>(s);
            a - l
        }
        SDS_TYPE_64 => {
            let (l, a) = hdr::<u64>(s);
            a - l
        }
        _ => 0,
    }
}

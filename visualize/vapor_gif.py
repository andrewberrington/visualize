'''
    create an animated gif file from a series of vapor image captures using imageio,
    images MUST be in jpeg format and the name of the variable MUST
    be at the beginning of the image files (e.g. core0000.jpg)
    gif file will be saved to the same directory as the jpg images

    example: python vapor_gif.py -dir /Users/berringtonaca/vapor_images -v core -n core_ts -ts 0.5
'''

import imageio as iio
import glob
import argparse


def main(args):
    filenames = glob.glob(f'{args.dir}/{args.varname}*.jpg')
    imgs = []
    for filename in filenames:
        imgs.append(iio.imread(filename))
    iio.mimsave(f'{args.dir}/{args.imgname}.gif', imgs, duration=f'{args.timestep}')

if __name__ == "__main__":
    linebreaks = argparse.RawTextHelpFormatter
    descrip = __doc__.lstrip()
    parser = argparse.ArgumentParser(description=descrip,
                                     formatter_class=linebreaks)
    parser.add_argument('-dir', '--directory', dest='dir',
                        help='directory containing jpeg images to be animated', required=True)
    parser.add_argument('-v', '--varname', dest='varname',
                        help='name of the variable', required=True)
    parser.add_argument('-n', '--img_name', dest='imgname',
                        help='name of the outputted gif file', required=True)
    parser.add_argument('-ts', '--timestep', dest='timestep',
                        help='timestep between frames of the gif in sec',
                        required=True, type=float)
    args = parser.parse_args()
    main(args)